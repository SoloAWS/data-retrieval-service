from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional
from ....seedwork.domain.value_objects import ValueObject


class SourceType(Enum):
    HOSPITAL = "HOSPITAL"
    LABORATORY = "LABORATORY"
    CLINIC = "CLINIC"
    RESEARCH_CENTER = "RESEARCH_CENTER"


class ImageFormat(Enum):
    DICOM = "DICOM"
    JPEG = "JPEG"
    PNG = "PNG"
    TIFF = "TIFF"
    RAW = "RAW"


class RetrievalStatus(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class RetrievalMethod(Enum):
    SFTP = "SFTP"
    API = "API"
    DIRECT_UPLOAD = "DIRECT_UPLOAD"
    CLOUD_STORAGE = "CLOUD_STORAGE"


@dataclass(frozen=True)
class ImageMetadata(ValueObject):
    """Metadatos asociados a una imagen médica"""
    format: ImageFormat
    modality: str  # Rayos X, Resonancia, etc.
    region: str    # Parte del cuerpo
    size_bytes: int
    dimensions: Optional[str] = None  # Dimensiones de la imagen, como "1024x768"
    
    
@dataclass(frozen=True)
class SourceMetadata(ValueObject):
    """Metadatos asociados a una fuente de imágenes médicas"""
    source_type: SourceType
    source_name: str
    source_id: str
    location: str  # País/Región
    retrieval_method: RetrievalMethod


@dataclass(frozen=True)
class RetrievalResult(ValueObject):
    """Resultado de una tarea de recuperación de imágenes"""
    status: RetrievalStatus
    message: str
    total_images: int = 0
    successful_images: int = 0
    failed_images: int = 0
    details: Optional[List[dict]] = None