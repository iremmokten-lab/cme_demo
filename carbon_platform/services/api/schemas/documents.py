from pydantic import BaseModel

class DocumentOut(BaseModel):
    id: str
    file_name: str
    doc_type: str
    s3_key: str
    sha256: str

class PresignOut(BaseModel):
    url: str
    expires_seconds: int
