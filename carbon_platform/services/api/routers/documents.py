import uuid
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from sqlalchemy import insert, select
from services.api.routers.auth import get_current_db_with_rls
from services.api.db.models import Document
from services.api.core.storage import storage
from services.api.schemas.documents import DocumentOut, PresignOut
from services.api.core.audit import write_audit_log
from services.api.core.config import settings

router = APIRouter()

@router.post("/upload", response_model=DocumentOut)
async def upload_document(
    ctx_db=Depends(get_current_db_with_rls),
    doc_type: str = Form(...),
    file: UploadFile = File(...),
):
    ctx, db = ctx_db
    if not file.filename:
        raise HTTPException(status_code=400, detail="Dosya adı boş")

    # tenant-scoped key
    doc_id = uuid.uuid4()
    safe_name = file.filename.replace("/", "_").replace("\\", "_")
    key = f"documents/{ctx['tid']}/{doc_id}/{safe_name}"

    stored = await storage.put_fileobj(key, file.file)

    res = await db.execute(
        insert(Document).values(
            tenant_id=ctx["tid"],
            s3_key=stored.key,
            file_name=safe_name,
            doc_type=doc_type,
            sha256=stored.sha256,
            uploaded_by=ctx["uid"],
        ).returning(Document)
    )
    doc = res.scalar_one()
    await write_audit_log(db, ctx["tid"], ctx["uid"], "upload", "document", str(doc.id), None, {"doc_type": doc_type, "s3_key": stored.key, "sha256": stored.sha256})
    await db.commit()

    return DocumentOut(
        id=str(doc.id),
        file_name=doc.file_name,
        doc_type=doc.doc_type,
        s3_key=doc.s3_key,
        sha256=doc.sha256,
    )

@router.get("/", response_model=list[DocumentOut])
async def list_documents(ctx_db=Depends(get_current_db_with_rls), doc_type: str | None = None):
    ctx, db = ctx_db
    q = select(Document).where(Document.tenant_id == ctx["tid"])
    if doc_type:
        q = q.where(Document.doc_type == doc_type)
    res = await db.execute(q.order_by(Document.uploaded_at.desc()))
    items = res.scalars().all()
    return [
        DocumentOut(id=str(d.id), file_name=d.file_name, doc_type=d.doc_type, s3_key=d.s3_key, sha256=d.sha256)
        for d in items
    ]

@router.get("/{document_id}/presign", response_model=PresignOut)
async def presign_document(document_id: str, ctx_db=Depends(get_current_db_with_rls)):
    ctx, db = ctx_db
    res = await db.execute(select(Document).where(Document.tenant_id == ctx["tid"], Document.id == document_id))
    doc = res.scalar_one_or_none()
    if doc is None:
        raise HTTPException(status_code=404, detail="Doküman bulunamadı")
    url = storage.presign_get(doc.s3_key, expires_seconds=settings.PRESIGN_EXPIRES_SECONDS)
    return PresignOut(url=url, expires_seconds=settings.PRESIGN_EXPIRES_SECONDS)
