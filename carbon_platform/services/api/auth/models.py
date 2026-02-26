from sqlalchemy import Column, String, Boolean
from sqlalchemy.dialects.postgresql import UUID
import uuid

from services.api.database.db import Base


class User(Base):

    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    email = Column(String, unique=True, nullable=False)

    password_hash = Column(String, nullable=False)

    tenant_id = Column(String, nullable=False)

    role = Column(String, nullable=False)

    active = Column(Boolean, default=True)
