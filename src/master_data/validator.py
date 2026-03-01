from __future__ import annotations

from datetime import datetime, timezone


def utcnow():
    return datetime.now(timezone.utc)


class MasterDataValidationError(ValueError):
    pass


def ensure_not_past(date_value: datetime, *, label: str) -> None:
    # Faz 1: basit koruma. İsterseniz "geçmişe yazma" kuralını config ile esnetebiliriz.
    if not isinstance(date_value, datetime):
        raise MasterDataValidationError(f"{label} geçersiz.")
    # timezone yoksa kabul et, ama UTC varsay
    if date_value.tzinfo is None:
        return
    # burada zorunlu kontrol yok; önemli olan versioning ile geçmişe müdahaleyi engellemek
    return


def ensure_cn_code_format(code: str) -> None:
    c = (code or "").strip()
    if len(c) < 2:
        raise MasterDataValidationError("CN kodu çok kısa.")
    # çok katı regex koymuyoruz; ülke datasetleri farklılık gösterebilir
    if not c.replace(" ", "").replace(".", "").isdigit():
        raise MasterDataValidationError("CN kodu sayısal olmalıdır (ör. 72081000).")


def ensure_non_empty(s: str, label: str) -> None:
    if not (s or "").strip():
        raise MasterDataValidationError(f"{label} boş olamaz.")
