from pydantic import BaseModel

class ProductCreate(BaseModel):
    facility_id: str
    product_code: str
    name: str
    unit: str = "ton"
    cn_code: str | None = None

class ProductOut(BaseModel):
    id: str
    facility_id: str
    product_code: str
    name: str
    unit: str
    cn_code: str | None

class MaterialCreate(BaseModel):
    material_code: str
    name: str
    unit: str = "ton"
    embedded_factor_id: str | None = None

class MaterialOut(BaseModel):
    id: str
    material_code: str
    name: str
    unit: str
    embedded_factor_id: str | None
