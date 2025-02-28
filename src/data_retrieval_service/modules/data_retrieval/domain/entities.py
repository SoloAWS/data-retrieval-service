from dataclasses import dataclass, field
from datetime import datetime
import uuid
from typing import List, Optional, Dict

from .events import RetrievalStarted, RetrievalCompleted, RetrievalFailed, ImagesRetrieved, ImageReadyForAnonymization
from ....seedwork.domain.entities import Entity
from ....seedwork.domain.aggregate import AggregateRoot
from .value_objects import ImageMetadata, SourceMetadata, RetrievalResult, RetrievalStatus


@dataclass
class ImageData(Entity):
    """
    Representa una imagen médica dentro del sistema.
    """
    metadata: ImageMetadata = field(default=None)
    filename: str = field(default=None)
    file_path: str = field(default=None) # Ruta donde se almacena la imagen en disco
    size_bytes: int = field(default=0) # Tamaño en bytes
    is_stored: bool = False
    created_at: datetime = field(default=None)
    updated_at: datetime = field(default=None)


@dataclass
class RetrievalTask(AggregateRoot):
    """
    Representa una tarea de recuperación de imágenes médicas.
    Este es el agregado raíz que coordina el proceso de recuperación
    de imágenes desde una fuente.
    """
    source_metadata: SourceMetadata = field(default=None)
    batch_id: str = field(default=None)
    priority: int = field(default=0)
    storage_path: str = field(default=None)# Ruta base donde se almacenarán las imágenes
    images: List[ImageData] = field(default_factory=list)
    result: Optional[RetrievalResult] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    metadata: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        if not hasattr(self, 'events'):
            self.events = []

    def start_retrieval(self):
        """Inicia la tarea de recuperación de imágenes"""
        
        self.started_at = datetime.now()
        
        self.result = RetrievalResult(
            status=RetrievalStatus.IN_PROGRESS,
            message="Retrieval task started",
        )
        
        event = RetrievalStarted(
            task_id=self.id, 
            source_metadata=self.source_metadata, 
            batch_id=self.batch_id,
            timestamp=datetime.now()
        )
        self.add_event(event)
        return event

    def add_image(self, image_data: ImageData):
        """Añade una imagen recuperada a la tarea"""
        self.images.append(image_data)
        
        # No publicamos directamente un evento aquí, esperamos a tener un lote
        return image_data
        
    def notify_images_retrieved(self, images: List[ImageData]):
        """Notifica que un lote de imágenes ha sido recuperado"""
        image_ids = [img.id for img in images]
    
        event = ImagesRetrieved(
            task_id=self.id,
            source=self.source_metadata.source_name,
            number_of_images=len(images),
            batch_id=self.batch_id,
            image_ids=image_ids
        )
        self.add_event(event)

        # Crear eventos de imágenes listas para anonimización
        for image in images:
            ano_event = ImageReadyForAnonymization(
                image_id=image.id,
                task_id=self.id, 
                source=self.source_metadata.source_name,
                modality=image.metadata.modality,
                region=image.metadata.region,
                file_path=image.file_path
            )
            self.add_event(ano_event)
        
        return event
        
    def get_total_size(self) -> int:
        """Obtiene el tamaño total en bytes de todas las imágenes recuperadas"""
        return sum(img.size_bytes for img in self.images)
        
    def get_images_count(self) -> int:
        """Obtiene el número de imágenes recuperadas"""
        return len(self.images)

    def complete_retrieval(self, successful_images: int, failed_images: int, details: List[dict] = None):
        """Completa la tarea de recuperación con éxito"""
        self.completed_at = datetime.now()
        total_images = successful_images + failed_images
        
        self.result = RetrievalResult(
            status=RetrievalStatus.COMPLETED,
            message="Retrieval completed successfully",
            total_images=total_images,
            successful_images=successful_images,
            failed_images=failed_images,
            details=details
        )
        
        event = RetrievalCompleted(
            task_id=self.id, 
            result=self.result, 
            source=self.source_metadata.source_name,
            location=self.source_metadata.location
        )
        self.add_event(event)
        return event

    def fail_retrieval(self, error_message: str, details: List[dict] = None):
        """Marca la tarea de recuperación como fallida"""
        self.completed_at = datetime.now()
        
        # Contar imágenes exitosas hasta el momento de la falla
        successful_images = len(self.images)
        
        self.result = RetrievalResult(
            status=RetrievalStatus.FAILED,
            message=error_message,
            total_images=successful_images,  # Total recuperado hasta la falla
            successful_images=successful_images,
            failed_images=0,  # No sabemos cuántas fallaron en total
            details=details
        )
        
        event = RetrievalFailed(
            task_id=self.id, 
            error_message=error_message,
            source=self.source_metadata.source_name
        )
        self.add_event(event)
        return event