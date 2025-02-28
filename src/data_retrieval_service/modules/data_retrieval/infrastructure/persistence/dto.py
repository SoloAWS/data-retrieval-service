from sqlalchemy import Column, String, Integer, JSON, ForeignKey, Boolean, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime

from .....config.database import Base


class RetrievalTaskDTO(Base):
    """DTO para la entidad RetrievalTask"""
    __tablename__ = "retrieval_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True)
    batch_id = Column(String, nullable=False)
    source_type = Column(String, nullable=False)
    source_name = Column(String, nullable=False)
    source_id = Column(String, nullable=False)
    location = Column(String, nullable=False)
    retrieval_method = Column(String, nullable=False)
    priority = Column(Integer, default=0)
    storage_path = Column(String, nullable=False)
    
    # Estados y tiempos
    status = Column(String, nullable=False, default="PENDING")
    message = Column(String)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    
    # Resultados
    total_images = Column(Integer, default=0)
    successful_images = Column(Integer, default=0)
    failed_images = Column(Integer, default=0)
    details = Column(JSON)
    
    # Metadatos adicionales
    _metadata = Column(JSON)
    
    # Relación con imágenes
    images = relationship("ImageDataDTO", back_populates="task", cascade="all, delete-orphan")


class ImageDataDTO(Base):
    """DTO para la entidad ImageData"""
    __tablename__ = "images"

    id = Column(UUID(as_uuid=True), primary_key=True)
    task_id = Column(UUID(as_uuid=True), ForeignKey("retrieval_tasks.id"), nullable=False)
    filename = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    
    # Metadatos de la imagen
    format = Column(String, nullable=False)
    modality = Column(String, nullable=False)
    region = Column(String, nullable=False)
    size_bytes = Column(Integer, nullable=False)
    dimensions = Column(String)
    is_stored = Column(Boolean, default=False)
    
    # Fechas
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relación con tarea
    task = relationship("RetrievalTaskDTO", back_populates="images")