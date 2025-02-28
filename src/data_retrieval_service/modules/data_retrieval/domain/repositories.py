from abc import ABC, abstractmethod
from typing import List, Optional
import uuid

from .entities import RetrievalTask, ImageData


class RetrievalRepository(ABC):
    """Interfaz para el repositorio de tareas de recuperación"""
    
    @abstractmethod
    async def get_by_id(self, task_id: uuid.UUID) -> Optional[RetrievalTask]:
        """Obtiene una tarea de recuperación por su ID"""
        pass

    @abstractmethod
    async def save(self, task: RetrievalTask) -> None:
        """Guarda una tarea de recuperación"""
        pass

    @abstractmethod
    async def update(self, task: RetrievalTask) -> None:
        """Actualiza una tarea de recuperación existente"""
        pass
    
    @abstractmethod
    async def get_pending_tasks(self) -> List[RetrievalTask]:
        """Obtiene las tareas pendientes ordenadas por prioridad"""
        pass
    
    @abstractmethod
    async def get_tasks_by_source(self, source_id: str, limit: int = 10) -> List[RetrievalTask]:
        """Obtiene tareas de una fuente específica"""
        pass
    
    @abstractmethod
    async def get_tasks_by_batch(self, batch_id: str) -> List[RetrievalTask]:
        """Obtiene tareas de un lote específico"""
        pass


class ImageRepository(ABC):
    """Interfaz para el repositorio de imágenes"""
    
    @abstractmethod
    async def save(self, image: ImageData, task_id: uuid.UUID) -> None:
        """Guarda una imagen"""
        pass
    
    @abstractmethod
    async def save_batch(self, images: List[ImageData], task_id: uuid.UUID) -> None:
        """Guarda un lote de imágenes"""
        pass
    
    @abstractmethod
    async def get_by_id(self, image_id: uuid.UUID) -> Optional[ImageData]:
        """Obtiene una imagen por su ID"""
        pass
    
    @abstractmethod
    async def get_images_by_task(self, task_id: uuid.UUID) -> List[ImageData]:
        """Obtiene todas las imágenes asociadas a una tarea"""
        pass
    
    @abstractmethod
    async def get_images_count_by_task(self, task_id: uuid.UUID) -> int:
        """Obtiene el número de imágenes asociadas a una tarea"""
        pass
    
    @abstractmethod
    async def update_image_status(self, image_id: uuid.UUID, is_stored: bool) -> None:
        """Actualiza el estado de almacenamiento de una imagen"""
        pass