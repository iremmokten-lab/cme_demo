from __future__ import annotations

from sqlalchemy.orm import Session

from src.db.models import Facility
from src.master_data import models


class MasterDataRepository:
    def __init__(self, s: Session):
        self.s = s

    # ---------- Facilities (core + version overlay) ----------
    def create_core_facility(self, company_id: int, name: str, country: str, sector: str) -> Facility:
        fac = Facility(company_id=company_id, name=name, country=country, sector=sector)
        self.s.add(fac)
        self.s.flush()
        return fac

    def get_core_facility(self, facility_id: int) -> Facility | None:
        return self.s.get(Facility, int(facility_id))

    def list_core_facilities(self, company_id: int) -> list[Facility]:
        return (
            self.s.query(Facility)
            .filter(Facility.company_id == int(company_id))
            .order_by(Facility.id.desc())
            .all()
        )

    def get_active_facility_version(self, company_id: int, facility_id: int) -> models.MasterFacilityVersion | None:
        return (
            self.s.query(models.MasterFacilityVersion)
            .filter(
                models.MasterFacilityVersion.company_id == int(company_id),
                models.MasterFacilityVersion.facility_id == int(facility_id),
                models.MasterFacilityVersion.is_active.is_(True),
            )
            .order_by(models.MasterFacilityVersion.version.desc())
            .first()
        )

    def create_facility_version(self, v: models.MasterFacilityVersion) -> models.MasterFacilityVersion:
        self.s.add(v)
        self.s.flush()
        return v

    # ---------- Products ----------
    def get_active_product(self, company_id: int, logical_id: str) -> models.MasterProduct | None:
        return (
            self.s.query(models.MasterProduct)
            .filter(
                models.MasterProduct.company_id == int(company_id),
                models.MasterProduct.logical_id == str(logical_id),
                models.MasterProduct.is_active.is_(True),
            )
            .order_by(models.MasterProduct.version.desc())
            .first()
        )

    def list_active_products(self, company_id: int) -> list[models.MasterProduct]:
        return (
            self.s.query(models.MasterProduct)
            .filter(models.MasterProduct.company_id == int(company_id), models.MasterProduct.is_active.is_(True))
            .order_by(models.MasterProduct.id.desc())
            .all()
        )

    def create_product(self, p: models.MasterProduct) -> models.MasterProduct:
        self.s.add(p)
        self.s.flush()
        return p

    # ---------- CN Codes ----------
    def get_cn_code(self, code: str) -> models.MasterCNCode | None:
        return self.s.get(models.MasterCNCode, str(code))

    def upsert_cn_code(self, cn: models.MasterCNCode) -> models.MasterCNCode:
        # merge: insert or update
        cn2 = self.s.merge(cn)
        self.s.flush()
        return cn2

    def list_cn_codes(self, limit: int = 500) -> list[models.MasterCNCode]:
        return self.s.query(models.MasterCNCode).order_by(models.MasterCNCode.code.asc()).limit(int(limit)).all()

    # ---------- BOM ----------
    def list_active_bom_edges(self, company_id: int) -> list[models.MasterBOMEdge]:
        return (
            self.s.query(models.MasterBOMEdge)
            .filter(models.MasterBOMEdge.company_id == int(company_id), models.MasterBOMEdge.is_active.is_(True))
            .order_by(models.MasterBOMEdge.id.desc())
            .all()
        )

    def get_active_edge(self, company_id: int, parent_id: int, child_id: int) -> models.MasterBOMEdge | None:
        return (
            self.s.query(models.MasterBOMEdge)
            .filter(
                models.MasterBOMEdge.company_id == int(company_id),
                models.MasterBOMEdge.parent_product_id == int(parent_id),
                models.MasterBOMEdge.child_product_id == int(child_id),
                models.MasterBOMEdge.is_active.is_(True),
            )
            .order_by(models.MasterBOMEdge.version.desc())
            .first()
        )

    def create_edge(self, e: models.MasterBOMEdge) -> models.MasterBOMEdge:
        self.s.add(e)
        self.s.flush()
        return e

    # ---------- Change log ----------
    def add_change(self, ch: models.MasterDataChange) -> None:
        self.s.add(ch)
        self.s.flush()
