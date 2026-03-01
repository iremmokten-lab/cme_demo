from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from sqlalchemy.orm import Session

from src.master_data import models
from src.master_data.bom_graph import BOMGraph, Edge
from src.master_data.hashing import sha256_hex
from src.master_data.repository import MasterDataRepository
from src.master_data.schemas import BOMEdgeUpsert, CNCodeUpsert, FacilityUpsert, ProductUpsert
from src.master_data.validator import MasterDataValidationError, ensure_cn_code_format, ensure_non_empty


def _tz(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        # streamlit çoğunlukla naive datetime üretir; UTC varsay.
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _close_previous(prev_valid_from: datetime, new_valid_from: datetime) -> datetime:
    # geçmiş kaydı kapatırken "1 saniye önce" gibi bir kapanış yapalım.
    # Ama new_valid_from çok yakınsa negatif olmasın.
    new_vf = _tz(new_valid_from)
    pvf = _tz(prev_valid_from)
    if new_vf <= pvf:
        return new_vf
    return new_vf - timedelta(seconds=1)


class MasterDataService:
    """Faz 1 Master Data Engine.

    Amaç:
      - Facilities registry (version history)
      - Product master (versioned)
      - CN codes registry
      - BOM graph (versioned)
      - Change log (hash-based)
    """

    def __init__(self, s: Session, *, company_id: int, user_id: int | None = None):
        self.s = s
        self.company_id = int(company_id)
        self.user_id = int(user_id) if user_id is not None else None
        self.repo = MasterDataRepository(s)

    # ----------------------------
    # Facilities
    # ----------------------------
    def upsert_facility(self, payload: FacilityUpsert) -> Dict[str, Any]:
        ensure_non_empty(payload.name, "Tesis adı")

        valid_from = _tz(payload.valid_from)

        if payload.facility_id is None:
            core = self.repo.create_core_facility(
                self.company_id, payload.name.strip(), payload.country.strip(), payload.sector.strip()
            )
            version = models.MasterFacilityVersion(
                company_id=self.company_id,
                facility_id=int(core.id),
                name=payload.name.strip(),
                country=payload.country.strip(),
                sector=payload.sector.strip(),
                version=1,
                valid_from=valid_from,
                valid_to=None,
                is_active=True,
            )
            self.repo.create_facility_version(version)
            self._log_change(
                entity_type="facility",
                entity_logical_id=str(core.id),
                operation="create",
                old_obj=None,
                new_obj=self._facility_version_obj(version),
                note="Yeni tesis oluşturuldu.",
            )
            # core güncel kalsın
            core.name = payload.name.strip()
            core.country = payload.country.strip()
            core.sector = payload.sector.strip()
            return {"facility_id": core.id, "version": version.version}
        else:
            core = self.repo.get_core_facility(int(payload.facility_id))
            if not core or int(core.company_id) != self.company_id:
                raise MasterDataValidationError("Tesis bulunamadı veya bu şirkete ait değil.")

            prev = self.repo.get_active_facility_version(self.company_id, int(core.id))
            prev_obj = self._facility_version_obj(prev) if prev else None

            new_version_num = (int(prev.version) + 1) if prev else 1

            if prev:
                prev.is_active = False
                prev.valid_to = _close_previous(prev.valid_from, valid_from)

            v = models.MasterFacilityVersion(
                company_id=self.company_id,
                facility_id=int(core.id),
                name=payload.name.strip(),
                country=payload.country.strip(),
                sector=payload.sector.strip(),
                version=new_version_num,
                valid_from=valid_from,
                valid_to=None,
                is_active=True,
            )
            self.repo.create_facility_version(v)

            self._log_change(
                entity_type="facility",
                entity_logical_id=str(core.id),
                operation="update",
                old_obj=prev_obj,
                new_obj=self._facility_version_obj(v),
                note="Tesis bilgisi güncellendi (yeni versiyon).",
            )

            # core güncel kalsın (operasyonel)
            core.name = payload.name.strip()
            core.country = payload.country.strip()
            core.sector = payload.sector.strip()
            return {"facility_id": core.id, "version": v.version}

    def list_facilities(self) -> list[Dict[str, Any]]:
        core_list = self.repo.list_core_facilities(self.company_id)
        out = []
        for fac in core_list:
            v = self.repo.get_active_facility_version(self.company_id, int(fac.id))
            if v:
                out.append(
                    {
                        "facility_id": fac.id,
                        "name": v.name,
                        "country": v.country,
                        "sector": v.sector,
                        "version": v.version,
                        "valid_from": v.valid_from,
                    }
                )
            else:
                out.append(
                    {
                        "facility_id": fac.id,
                        "name": fac.name,
                        "country": fac.country,
                        "sector": fac.sector,
                        "version": 0,
                        "valid_from": None,
                    }
                )
        return out

    # ----------------------------
    # Products
    # ----------------------------
    def upsert_product(self, payload: ProductUpsert) -> Dict[str, Any]:
        ensure_non_empty(payload.name, "Ürün adı")
        ensure_cn_code_format(payload.cn_code)

        valid_from = _tz(payload.valid_from)

        if not payload.logical_id:
            logical_id = str(uuid.uuid4())
            p = models.MasterProduct(
                company_id=self.company_id,
                logical_id=logical_id,
                name=payload.name.strip(),
                cn_code=payload.cn_code.strip(),
                sector=(payload.sector or "").strip(),
                valid_from=valid_from,
                valid_to=None,
                version=1,
                is_active=True,
            )
            self.repo.create_product(p)
            self._log_change(
                entity_type="product",
                entity_logical_id=logical_id,
                operation="create",
                old_obj=None,
                new_obj=self._product_obj(p),
                note="Yeni ürün oluşturuldu.",
            )
            return {"logical_id": logical_id, "id": p.id, "version": p.version}

        logical_id = str(payload.logical_id).strip()
        prev = self.repo.get_active_product(self.company_id, logical_id)
        if not prev:
            # logical_id var ama aktif kayıt yoksa: yeni başlat
            p = models.MasterProduct(
                company_id=self.company_id,
                logical_id=logical_id,
                name=payload.name.strip(),
                cn_code=payload.cn_code.strip(),
                sector=(payload.sector or "").strip(),
                valid_from=valid_from,
                valid_to=None,
                version=1,
                is_active=True,
            )
            self.repo.create_product(p)
            self._log_change(
                entity_type="product",
                entity_logical_id=logical_id,
                operation="create",
                old_obj=None,
                new_obj=self._product_obj(p),
                note="Ürün logical_id ile yeniden oluşturuldu (aktif kayıt yoktu).",
            )
            return {"logical_id": logical_id, "id": p.id, "version": p.version}

        prev_obj = self._product_obj(prev)
        prev.is_active = False
        prev.valid_to = _close_previous(prev.valid_from, valid_from)

        p2 = models.MasterProduct(
            company_id=self.company_id,
            logical_id=logical_id,
            name=payload.name.strip(),
            cn_code=payload.cn_code.strip(),
            sector=(payload.sector or "").strip(),
            valid_from=valid_from,
            valid_to=None,
            version=int(prev.version) + 1,
            is_active=True,
        )
        self.repo.create_product(p2)

        self._log_change(
            entity_type="product",
            entity_logical_id=logical_id,
            operation="update",
            old_obj=prev_obj,
            new_obj=self._product_obj(p2),
            note="Ürün güncellendi (yeni versiyon).",
        )
        return {"logical_id": logical_id, "id": p2.id, "version": p2.version}

    def list_products(self) -> list[Dict[str, Any]]:
        prods = self.repo.list_active_products(self.company_id)
        return [
            {
                "id": p.id,
                "logical_id": p.logical_id,
                "name": p.name,
                "cn_code": p.cn_code,
                "sector": p.sector,
                "version": p.version,
                "valid_from": p.valid_from,
            }
            for p in prods
        ]

    # ----------------------------
    # CN Codes (global)
    # ----------------------------
    def upsert_cn_code(self, payload: CNCodeUpsert) -> Dict[str, Any]:
        ensure_cn_code_format(payload.code)
        cn = models.MasterCNCode(
            code=payload.code.strip(),
            description=(payload.description or "").strip(),
            sector=(payload.sector or "").strip(),
            source=(payload.source or "").strip(),
            valid_from=_tz(payload.valid_from) if payload.valid_from else None,
            valid_to=_tz(payload.valid_to) if payload.valid_to else None,
        )
        prev = self.repo.get_cn_code(cn.code)
        prev_obj = self._cn_obj(prev) if prev else None
        self.repo.upsert_cn_code(cn)
        self._log_change(
            entity_type="cn_code",
            entity_logical_id=cn.code,
            operation="update" if prev else "create",
            old_obj=prev_obj,
            new_obj=self._cn_obj(cn),
            note="CN kodu kaydedildi.",
        )
        return {"code": cn.code}

    def list_cn_codes(self, limit: int = 200) -> list[Dict[str, Any]]:
        codes = self.repo.list_cn_codes(limit=limit)
        return [
            {
                "code": c.code,
                "description": c.description,
                "sector": c.sector,
                "source": c.source,
                "valid_from": c.valid_from,
                "valid_to": c.valid_to,
            }
            for c in codes
        ]

    # ----------------------------
    # BOM
    # ----------------------------
    def upsert_bom_edge(self, payload: BOMEdgeUpsert) -> Dict[str, Any]:
        if int(payload.parent_product_id) == int(payload.child_product_id):
            raise MasterDataValidationError("BOM: parent ve child aynı olamaz.")

        valid_from = _tz(payload.valid_from)

        # aynı pair için aktif edge var mı?
        prev = self.repo.get_active_edge(self.company_id, payload.parent_product_id, payload.child_product_id)
        prev_obj = self._edge_obj(prev) if prev else None

        if prev:
            prev.is_active = False
            prev.valid_to = _close_previous(prev.valid_from, valid_from)

        e = models.MasterBOMEdge(
            company_id=self.company_id,
            parent_product_id=int(payload.parent_product_id),
            child_product_id=int(payload.child_product_id),
            ratio=float(payload.ratio),
            unit=(payload.unit or "kg").strip(),
            valid_from=valid_from,
            valid_to=None,
            version=(int(prev.version) + 1) if prev else 1,
            is_active=True,
        )
        self.repo.create_edge(e)

        # cycle check: tüm aktif edge'lerle kontrol et
        edges = self.repo.list_active_bom_edges(self.company_id)
        g = BOMGraph([Edge(parent_id=x.parent_product_id, child_id=x.child_product_id) for x in edges])
        if g.has_cycle():
            # rollback edge
            raise MasterDataValidationError("BOM döngü (cycle) oluşturuyor. İşlem iptal edildi.")

        self._log_change(
            entity_type="bom",
            entity_logical_id=f"{e.parent_product_id}->{e.child_product_id}",
            operation="update" if prev else "create",
            old_obj=prev_obj,
            new_obj=self._edge_obj(e),
            note="BOM ilişkisi kaydedildi.",
        )
        return {"edge_id": e.id, "version": e.version}

    def list_bom_edges(self) -> list[Dict[str, Any]]:
        edges = self.repo.list_active_bom_edges(self.company_id)
        return [
            {
                "id": e.id,
                "parent_product_id": e.parent_product_id,
                "child_product_id": e.child_product_id,
                "ratio": e.ratio,
                "unit": e.unit,
                "version": e.version,
                "valid_from": e.valid_from,
            }
            for e in edges
        ]

    # ----------------------------
    # Change log
    # ----------------------------
    def list_changes(self, limit: int = 200) -> list[Dict[str, Any]]:
        q = (
            self.s.query(models.MasterDataChange)
            .filter(models.MasterDataChange.company_id == self.company_id)
            .order_by(models.MasterDataChange.created_at.desc())
            .limit(int(limit))
        )
        rows = q.all()
        return [
            {
                "time": r.created_at,
                "entity_type": r.entity_type,
                "entity_id": r.entity_logical_id,
                "operation": r.operation,
                "old_hash": r.old_hash,
                "new_hash": r.new_hash,
                "note": r.note,
                "user_id": r.user_id,
            }
            for r in rows
        ]

    # ----------------------------
    # Helpers
    # ----------------------------
    def _log_change(self, *, entity_type: str, entity_logical_id: str, operation: str, old_obj: Any, new_obj: Any, note: str) -> None:
        ch = models.MasterDataChange(
            company_id=self.company_id,
            user_id=self.user_id,
            entity_type=str(entity_type),
            entity_logical_id=str(entity_logical_id),
            operation=str(operation),
            old_hash=sha256_hex(old_obj) if old_obj is not None else "",
            new_hash=sha256_hex(new_obj) if new_obj is not None else "",
            note=str(note or ""),
        )
        self.repo.add_change(ch)

    @staticmethod
    def _product_obj(p: models.MasterProduct | None) -> Dict[str, Any] | None:
        if not p:
            return None
        return {
            "logical_id": p.logical_id,
            "name": p.name,
            "cn_code": p.cn_code,
            "sector": p.sector,
            "version": p.version,
            "valid_from": p.valid_from.isoformat() if p.valid_from else None,
            "valid_to": p.valid_to.isoformat() if p.valid_to else None,
        }

    @staticmethod
    def _facility_version_obj(v: models.MasterFacilityVersion | None) -> Dict[str, Any] | None:
        if not v:
            return None
        return {
            "facility_id": v.facility_id,
            "name": v.name,
            "country": v.country,
            "sector": v.sector,
            "version": v.version,
            "valid_from": v.valid_from.isoformat() if v.valid_from else None,
            "valid_to": v.valid_to.isoformat() if v.valid_to else None,
        }

    @staticmethod
    def _cn_obj(c: models.MasterCNCode | None) -> Dict[str, Any] | None:
        if not c:
            return None
        return {
            "code": c.code,
            "description": c.description,
            "sector": c.sector,
            "source": c.source,
            "valid_from": c.valid_from.isoformat() if c.valid_from else None,
            "valid_to": c.valid_to.isoformat() if c.valid_to else None,
        }

    @staticmethod
    def _edge_obj(e: models.MasterBOMEdge | None) -> Dict[str, Any] | None:
        if not e:
            return None
        return {
            "parent_product_id": e.parent_product_id,
            "child_product_id": e.child_product_id,
            "ratio": e.ratio,
            "unit": e.unit,
            "version": e.version,
            "valid_from": e.valid_from.isoformat() if e.valid_from else None,
            "valid_to": e.valid_to.isoformat() if e.valid_to else None,
        }
