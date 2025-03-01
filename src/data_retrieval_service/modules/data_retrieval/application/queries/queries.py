from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime

from .....seedwork.application.queries import Query, QueryResult, QueryHandler
from ...domain.repositories import RetrievalRepository, ImageRepository
from ...domain.value_objects import RetrievalStatus


@dataclass
class GetRetrievalTaskById(Query):
    """Query para obtener una tarea de recuperación por su ID"""
    task_id: uuid.UUID


@dataclass
class GetPendingRetrievalTasks(Query):
    """Query para obtener todas las tareas pendientes"""
    pass


@dataclass
class GetTasksBySource(Query):
    """Query para obtener tareas por fuente"""
    source_id: str
    limit: int = 10


@dataclass
class GetTasksByBatch(Query):
    """Query para obtener tareas por lote"""
    batch_id: str


@dataclass
class GetImagesByTask(Query):
    """Query para obtener imágenes asociadas a una tarea"""
    task_id: uuid.UUID


@dataclass
class TaskDTO(QueryResult):
    """DTO para representar una tarea de recuperación"""
    id: str = ""
    batch_id: str = ""
    source_type: str = ""
    source_name: str = ""
    source_id: str = ""
    location: str = ""
    retrieval_method: str = ""
    priority: int = 0
    storage_path: str = ""
    status: str = "PENDING"
    message: Optional[str] = None
    total_images: int = 0
    successful_images: int = 0
    failed_images: int = 0
    details: Optional[List[dict]] = None
    created_at: str = ""
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    images_count: int = 0
    result: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert DTO to dictionary"""
        # Use __dict__ to convert to dictionary
        return {k: v for k, v in self.__dict__.items() if k != 'result_'}


@dataclass
class ImageDTO(QueryResult):
    """DTO para representar una imagen"""
    id: str = ""
    task_id: str = ""
    filename: str = ""
    file_path: str = ""
    format: str = ""
    modality: str = ""
    region: str = ""
    size_bytes: int = 0
    dimensions: Optional[str] = ""
    is_stored: bool = False
    created_at: str = ""
    updated_at: str = ""
    result: Optional[Dict[str, Any]] = None  # Adding result field with default value
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert DTO to dictionary"""
        # Return dictionary without the result field
        result_dict = {k: v for k, v in self.__dict__.items() if k != 'result'}
        return result_dict


class GetRetrievalTaskByIdHandler(QueryHandler):
    """Handler para obtener una tarea de recuperación por su ID"""
    
    def __init__(self, retrieval_repository: RetrievalRepository, image_repository: ImageRepository):
        self.retrieval_repository = retrieval_repository
        self.image_repository = image_repository

    async def handle(self, query: GetRetrievalTaskById) -> Optional[Dict[str, Any]]:
        task = await self.retrieval_repository.get_by_id(query.task_id)
        if not task:
            return None
            
        # Obtener conteo de imágenes
        images_count = await self.image_repository.get_images_count_by_task(task.id)
        
        # Construir el DTO de resultado
        result = None
        if task.result:
            result = {
                "status": task.result.status.value,
                "message": task.result.message,
                "total_images": task.result.total_images,
                "successful_images": task.result.successful_images,
                "failed_images": task.result.failed_images,
                "details": task.result.details
            }
        
        dto = TaskDTO(
            id=str(task.id),
            batch_id=task.batch_id,
            source_type=task.source_metadata.source_type.value,
            source_name=task.source_metadata.source_name,
            source_id=task.source_metadata.source_id,
            location=task.source_metadata.location,
            retrieval_method=task.source_metadata.retrieval_method.value,
            priority=task.priority,
            storage_path=task.storage_path,
            status=task.result.status.value if task.result else RetrievalStatus.PENDING.value,
            message=task.result.message if task.result else None,
            total_images=task.result.total_images if task.result else 0,
            successful_images=task.result.successful_images if task.result else 0,
            failed_images=task.result.failed_images if task.result else 0,
            details=task.result.details if task.result else None,
            created_at=task.created_at.isoformat() if task.created_at else None,
            started_at=task.started_at.isoformat() if task.started_at else None,
            completed_at=task.completed_at.isoformat() if task.completed_at else None,
            images_count=images_count,
            result=result
        )
        
        # Return as dictionary
        return dto.to_dict()


class GetPendingRetrievalTasksHandler(QueryHandler):
    """Handler para obtener todas las tareas pendientes"""
    
    def __init__(self, retrieval_repository: RetrievalRepository, image_repository: ImageRepository):
        self.retrieval_repository = retrieval_repository
        self.image_repository = image_repository

    async def handle(self, query: GetPendingRetrievalTasks) -> List[Dict[str, Any]]:
        tasks = await self.retrieval_repository.get_pending_tasks()
        result = []
        
        for task in tasks:
            # Obtener conteo de imágenes
            images_count = await self.image_repository.get_images_count_by_task(task.id)
            
            dto = TaskDTO(
                id=str(task.id),
                batch_id=task.batch_id,
                source_type=task.source_metadata.source_type.value,
                source_name=task.source_metadata.source_name,
                source_id=task.source_metadata.source_id,
                location=task.source_metadata.location,
                retrieval_method=task.source_metadata.retrieval_method.value,
                priority=task.priority,
                storage_path=task.storage_path,
                status=task.result.status.value if task.result else RetrievalStatus.PENDING.value,
                message=task.result.message if task.result else None,
                total_images=task.result.total_images if task.result else 0,
                successful_images=task.result.successful_images if task.result else 0,
                failed_images=task.result.failed_images if task.result else 0,
                details=task.result.details if task.result else None,
                created_at=task.created_at.isoformat() if task.created_at else None,
                started_at=task.started_at.isoformat() if task.started_at else None,
                completed_at=task.completed_at.isoformat() if task.completed_at else None,
                images_count=images_count
            )
            
            result.append(dto.to_dict())
            
        return result


class GetTasksBySourceHandler(QueryHandler):
    """Handler para obtener tareas por fuente"""
    
    def __init__(self, retrieval_repository: RetrievalRepository, image_repository: ImageRepository):
        self.retrieval_repository = retrieval_repository
        self.image_repository = image_repository

    async def handle(self, query: GetTasksBySource) -> List[Dict[str, Any]]:
        tasks = await self.retrieval_repository.get_tasks_by_source(query.source_id, query.limit)
        result = []
        
        for task in tasks:
            # Obtener conteo de imágenes
            images_count = await self.image_repository.get_images_count_by_task(task.id)
            
            dto = TaskDTO(
                id=str(task.id),
                batch_id=task.batch_id,
                source_type=task.source_metadata.source_type.value,
                source_name=task.source_metadata.source_name,
                source_id=task.source_metadata.source_id,
                location=task.source_metadata.location,
                retrieval_method=task.source_metadata.retrieval_method.value,
                priority=task.priority,
                storage_path=task.storage_path,
                status=task.result.status.value if task.result else RetrievalStatus.PENDING.value,
                message=task.result.message if task.result else None,
                total_images=task.result.total_images if task.result else 0,
                successful_images=task.result.successful_images if task.result else 0,
                failed_images=task.result.failed_images if task.result else 0,
                details=task.result.details if task.result else None,
                created_at=task.created_at.isoformat() if task.created_at else None,
                started_at=task.started_at.isoformat() if task.started_at else None,
                completed_at=task.completed_at.isoformat() if task.completed_at else None,
                images_count=images_count
            )
            
            result.append(dto.to_dict())
            
        return result


class GetTasksByBatchHandler(QueryHandler):
    """Handler para obtener tareas por lote"""
    
    def __init__(self, retrieval_repository: RetrievalRepository, image_repository: ImageRepository):
        self.retrieval_repository = retrieval_repository
        self.image_repository = image_repository

    async def handle(self, query: GetTasksByBatch) -> List[Dict[str, Any]]:
        tasks = await self.retrieval_repository.get_tasks_by_batch(query.batch_id)
        result = []
        
        for task in tasks:
            # Obtener conteo de imágenes
            images_count = await self.image_repository.get_images_count_by_task(task.id)
            
            dto = TaskDTO(
                id=str(task.id),
                batch_id=task.batch_id,
                source_type=task.source_metadata.source_type.value,
                source_name=task.source_metadata.source_name,
                source_id=task.source_metadata.source_id,
                location=task.source_metadata.location,
                retrieval_method=task.source_metadata.retrieval_method.value,
                priority=task.priority,
                storage_path=task.storage_path,
                status=task.result.status.value if task.result else RetrievalStatus.PENDING.value,
                message=task.result.message if task.result else None,
                total_images=task.result.total_images if task.result else 0,
                successful_images=task.result.successful_images if task.result else 0,
                failed_images=task.result.failed_images if task.result else 0,
                details=task.result.details if task.result else None,
                created_at=task.created_at.isoformat() if task.created_at else None,
                started_at=task.started_at.isoformat() if task.started_at else None,
                completed_at=task.completed_at.isoformat() if task.completed_at else None,
                images_count=images_count
            )
            
            result.append(dto.to_dict())
            
        return result


class GetImagesByTaskHandler(QueryHandler):
    """Handler para obtener imágenes asociadas a una tarea"""
    
    def __init__(self, image_repository: ImageRepository):
        self.image_repository = image_repository

    async def handle(self, query: GetImagesByTask) -> List[Dict[str, Any]]:
        images = await self.image_repository.get_images_by_task(query.task_id)
        
        return [
            ImageDTO(
                id=str(image.id),
                task_id=str(query.task_id),
                filename=image.filename,
                file_path=image.file_path,
                format=image.metadata.format.value,
                modality=image.metadata.modality,
                region=image.metadata.region,
                size_bytes=image.size_bytes,
                dimensions=image.metadata.dimensions,
                is_stored=image.is_stored,
                created_at=image.created_at.isoformat() if image.created_at else None,
                updated_at=image.updated_at.isoformat() if image.updated_at else None
            ).to_dict()
            for image in images
        ]


# Funciones para ayudar a ejecutar las consultas
async def get_retrieval_task_by_id(
    handler: GetRetrievalTaskByIdHandler,
    task_id: uuid.UUID
) -> Optional[Dict[str, Any]]:
    """Ejecuta la consulta GetRetrievalTaskById"""
    query = GetRetrievalTaskById(task_id=task_id)
    return await handler.handle(query)


async def get_pending_retrieval_tasks(
    handler: GetPendingRetrievalTasksHandler
) -> List[Dict[str, Any]]:
    """Ejecuta la consulta GetPendingRetrievalTasks"""
    query = GetPendingRetrievalTasks()
    return await handler.handle(query)


async def get_tasks_by_source(
    handler: GetTasksBySourceHandler,
    source_id: str,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """Ejecuta la consulta GetTasksBySource"""
    query = GetTasksBySource(source_id=source_id, limit=limit)
    return await handler.handle(query)


async def get_tasks_by_batch(
    handler: GetTasksByBatchHandler,
    batch_id: str
) -> List[Dict[str, Any]]:
    """Ejecuta la consulta GetTasksByBatch"""
    query = GetTasksByBatch(batch_id=batch_id)
    return await handler.handle(query)


async def get_images_by_task(
    handler: GetImagesByTaskHandler,
    task_id: uuid.UUID
) -> List[Dict[str, Any]]:
    """Ejecuta la consulta GetImagesByTask"""
    query = GetImagesByTask(task_id=task_id)
    return await handler.handle(query)