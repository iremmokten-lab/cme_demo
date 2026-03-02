from __future__ import annotations
import json
from sqlalchemy import select
from src.db.session import db
from .models import MD_Product, MD_Material, MD_Process, MD_BOMItem, MD_ChangeLog

def _log(project_id:int, entity:str, entity_id:int, action:str, diff:dict, actor_user_id:int|None):
    with db() as s:
        s.add(MD_ChangeLog(project_id=int(project_id), entity=entity, entity_id=int(entity_id), action=action,
                           diff_json=json.dumps(diff or {}, ensure_ascii=False), actor_user_id=(int(actor_user_id) if actor_user_id else None)))
        s.commit()

def upsert_product(project_id:int, sku:str, name:str, cn_code:str="", uom:str="t", actor_user_id:int|None=None)->MD_Product:
    with db() as s:
        obj=s.execute(select(MD_Product).where(MD_Product.project_id==int(project_id), MD_Product.sku==sku)).scalars().first()
        if not obj:
            obj=MD_Product(project_id=int(project_id), sku=sku, name=name, cn_code=cn_code, uom=uom, is_active=True)
            s.add(obj); s.commit(); s.refresh(obj)
            _log(project_id,"product",obj.id,"create",{"sku":sku,"name":name,"cn_code":cn_code,"uom":uom},actor_user_id)
            return obj
        diff={}
        if obj.name!=name: diff["name"]=[obj.name,name]; obj.name=name
        if obj.cn_code!=cn_code: diff["cn_code"]=[obj.cn_code,cn_code]; obj.cn_code=cn_code
        if obj.uom!=uom: diff["uom"]=[obj.uom,uom]; obj.uom=uom
        s.commit()
        if diff: _log(project_id,"product",obj.id,"update",diff,actor_user_id)
        return obj

def list_products(project_id:int)->list[MD_Product]:
    with db() as s:
        return s.execute(select(MD_Product).where(MD_Product.project_id==int(project_id)).order_by(MD_Product.id.desc())).scalars().all()

def upsert_material(project_id:int, code:str, name:str, uom:str="t", actor_user_id:int|None=None)->MD_Material:
    with db() as s:
        obj=s.execute(select(MD_Material).where(MD_Material.project_id==int(project_id), MD_Material.code==code)).scalars().first()
        if not obj:
            obj=MD_Material(project_id=int(project_id), code=code, name=name, uom=uom, is_active=True)
            s.add(obj); s.commit(); s.refresh(obj)
            _log(project_id,"material",obj.id,"create",{"code":code,"name":name,"uom":uom},actor_user_id)
            return obj
        diff={}
        if obj.name!=name: diff["name"]=[obj.name,name]; obj.name=name
        if obj.uom!=uom: diff["uom"]=[obj.uom,uom]; obj.uom=uom
        s.commit()
        if diff: _log(project_id,"material",obj.id,"update",diff,actor_user_id)
        return obj

def list_materials(project_id:int)->list[MD_Material]:
    with db() as s:
        return s.execute(select(MD_Material).where(MD_Material.project_id==int(project_id)).order_by(MD_Material.id.desc())).scalars().all()

def upsert_process(project_id:int, code:str, name:str, sector:str="generic", actor_user_id:int|None=None)->MD_Process:
    with db() as s:
        obj=s.execute(select(MD_Process).where(MD_Process.project_id==int(project_id), MD_Process.code==code)).scalars().first()
        if not obj:
            obj=MD_Process(project_id=int(project_id), code=code, name=name, sector=sector)
            s.add(obj); s.commit(); s.refresh(obj)
            _log(project_id,"process",obj.id,"create",{"code":code,"name":name,"sector":sector},actor_user_id)
            return obj
        diff={}
        if obj.name!=name: diff["name"]=[obj.name,name]; obj.name=name
        if obj.sector!=sector: diff["sector"]=[obj.sector,sector]; obj.sector=sector
        s.commit()
        if diff: _log(project_id,"process",obj.id,"update",diff,actor_user_id)
        return obj

def list_processes(project_id:int)->list[MD_Process]:
    with db() as s:
        return s.execute(select(MD_Process).where(MD_Process.project_id==int(project_id)).order_by(MD_Process.id.desc())).scalars().all()

def set_bom_line(project_id:int, product_id:int, material_id:int, qty_per_unit:str, actor_user_id:int|None=None)->MD_BOMItem:
    with db() as s:
        obj=s.execute(select(MD_BOMItem).where(MD_BOMItem.project_id==int(project_id), MD_BOMItem.product_id==int(product_id), MD_BOMItem.material_id==int(material_id))).scalars().first()
        if not obj:
            obj=MD_BOMItem(project_id=int(project_id), product_id=int(product_id), material_id=int(material_id), qty_per_unit=str(qty_per_unit))
            s.add(obj); s.commit(); s.refresh(obj)
            _log(project_id,"bom_item",obj.id,"create",{"product_id":product_id,"material_id":material_id,"qty_per_unit":qty_per_unit},actor_user_id)
            return obj
        old=obj.qty_per_unit
        obj.qty_per_unit=str(qty_per_unit); s.commit()
        if old!=obj.qty_per_unit:
            _log(project_id,"bom_item",obj.id,"update",{"qty_per_unit":[old,obj.qty_per_unit]},actor_user_id)
        return obj

def list_bom(project_id:int)->list[MD_BOMItem]:
    with db() as s:
        return s.execute(select(MD_BOMItem).where(MD_BOMItem.project_id==int(project_id)).order_by(MD_BOMItem.id.desc())).scalars().all()
