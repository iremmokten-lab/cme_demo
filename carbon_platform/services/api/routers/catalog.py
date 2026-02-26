from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import insert, select
from services.api.routers.auth import get_current_db_with_rls
from services.api.db.models import Product, Material
from services.api.schemas.catalog import ProductCreate, ProductOut, MaterialCreate, MaterialOut
from services.api.core.audit import write_audit_log

router = APIRouter()

@router.post("/products", response_model=ProductOut)
async def create_product(data: ProductCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(Product).values(
            tenant_id=ctx["tid"],
            facility_id=data.facility_id,
            product_code=data.product_code,
            name=data.name,
            unit=data.unit,
            cn_code=data.cn_code,
        ).returning(Product)
    )
    p = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "product", str(p.id), None, data.model_dump())
    await db.commit()
    return ProductOut(
        id=str(p.id),
        facility_id=str(p.facility_id),
        product_code=p.product_code,
        name=p.name,
        unit=p.unit,
        cn_code=p.cn_code,
    )

@router.get("/products", response_model=list[ProductOut])
async def list_products(ctx_db=Depends(get_current_db_with_rls), facility_id: str | None = None):
    ctx, db = ctx_db
    q = select(Product).where(Product.tenant_id == ctx["tid"])
    if facility_id:
        q = q.where(Product.facility_id == facility_id)
    res = await db.execute(q.order_by(Product.product_code.asc()))
    items = res.scalars().all()
    return [
        ProductOut(
            id=str(x.id),
            facility_id=str(x.facility_id),
            product_code=x.product_code,
            name=x.name,
            unit=x.unit,
            cn_code=x.cn_code
        ) for x in items
    ]

@router.post("/materials", response_model=MaterialOut)
async def create_material(data: MaterialCreate, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(
        insert(Material).values(
            tenant_id=ctx["tid"],
            material_code=data.material_code,
            name=data.name,
            unit=data.unit,
            embedded_factor_id=data.embedded_factor_id,
        ).returning(Material)
    )
    m = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "create", "material", str(m.id), None, data.model_dump())
    await db.commit()
    return MaterialOut(
        id=str(m.id),
        material_code=m.material_code,
        name=m.name,
        unit=m.unit,
        embedded_factor_id=str(m.embedded_factor_id) if m.embedded_factor_id else None,
    )

@router.get("/materials", response_model=list[MaterialOut])
async def list_materials(ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(select(Material).where(Material.tenant_id == ctx["tid"]).order_by(Material.material_code.asc()))
    items = res.scalars().all()
    return [
        MaterialOut(
            id=str(x.id),
            material_code=x.material_code,
            name=x.name,
            unit=x.unit,
            embedded_factor_id=str(x.embedded_factor_id) if x.embedded_factor_id else None,
        ) for x in items
    ]
