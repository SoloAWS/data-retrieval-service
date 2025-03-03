from dataclasses import dataclass, field
from datetime import datetime
import uuid
from typing import List

from ....seedwork.domain.events import DomainEvent
from .value_objects import SourceMetadata, RetrievalResult

@dataclass
class RetrievalStarted(DomainEvent):
    task_id: uuid.UUID = field(default=None)
    timestamp: datetime = field(default_factory=datetime.now)
    source_metadata: SourceMetadata = field(default=None)
    batch_id: str = field(default=None)
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "task_id": str(self.task_id),
            "source_metadata": {
                "source_type": self.source_metadata.source_type.value,
                "source_name": self.source_metadata.source_name,
                "source_id": self.source_metadata.source_id,
                "location": self.source_metadata.location,
                "retrieval_method": self.source_metadata.retrieval_method.value
            },
            "batch_id": self.batch_id,
            "timestamp": self.timestamp.isoformat()
        }

@dataclass
class RetrievalCompleted(DomainEvent):
    task_id: uuid.UUID = field(default=None)
    result: RetrievalResult = field(default=None)
    source: str = field(default=None)
    location: str = field(default=None)
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "task_id": str(self.task_id),
            "result": {
                "status": self.result.status.value,
                "message": self.result.message,
                "total_images": self.result.total_images,
                "successful_images": self.result.successful_images,
                "failed_images": self.result.failed_images,
                "details": self.result.details
            },
            "source": self.source,
            "location": self.location
        }

@dataclass
class RetrievalFailed(DomainEvent):
    task_id: uuid.UUID = field(default=None)
    error_message: str = field(default=None)
    source: str = field(default=None)
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "task_id": str(self.task_id),
            "error_message": self.error_message,
            "source": self.source
        }

@dataclass
class ImagesRetrieved(DomainEvent):
    task_id: uuid.UUID = field(default=None)
    source: str = field(default=None)
    number_of_images: int = field(default=None)
    batch_id: str = field(default=None)
    image_ids: List[uuid.UUID] = field(default_factory=list)
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "task_id": str(self.task_id),
            "source": self.source,
            "number_of_images": self.number_of_images,
            "batch_id": self.batch_id,
            "image_ids": [str(img_id) for img_id in self.image_ids]
        }

@dataclass
class ImageReadyForAnonymization(DomainEvent):
    image_id: uuid.UUID = field(default=None)
    task_id: uuid.UUID = field(default=None)
    source: str = field(default=None)
    modality: str = field(default=None)
    region: str = field(default=None)
    file_path: str = field(default=None)
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "image_id": str(self.image_id),
            "task_id": str(self.task_id),
            "source": self.source,
            "modality": self.modality,
            "region": self.region,
            "file_path": self.file_path
        }

@dataclass
class ImageReadyForAnonymization(DomainEvent):
    image_id: uuid.UUID = field(default=None)
    task_id: uuid.UUID = field(default=None)
    source: str = field(default=None)
    modality: str = field(default=None)
    region: str = field(default=None)
    file_path: str = field(default=None)
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "image_id": str(self.image_id),
            "task_id": str(self.task_id),
            "source": self.source,
            "modality": self.modality,
            "region": self.region,
            "file_path": self.file_path
        }


@dataclass
class ImageUploadFailed(DomainEvent):
    task_id: uuid.UUID = field(default=None)
    filename: str = field(default=None)
    error_message: str = field(default=None)
    source: str = field(default=None)
    format: str = field(default=None)
    modality: str = field(default=None)
    region: str = field(default=None)
    stack_trace: str = field(default=None)
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "task_id": str(self.task_id),
            "filename": self.filename,
            "error_message": self.error_message,
            "source": self.source,
            "format": self.format,
            "modality": self.modality,
            "region": self.region,
            "stack_trace": self.stack_trace
        }