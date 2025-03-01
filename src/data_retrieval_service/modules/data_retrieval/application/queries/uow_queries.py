from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime

from .....seedwork.application.queries import Query, QueryResult, QueryHandler
from .....seedwork.infrastructure.uow import UnitOfWork
from ...domain.value_objects import RetrievalStatus
from ...domain.repositories import RetrievalRepository, ImageRepository

# Reutilizamos las definiciones de queries existentes
from .queries import (
    GetRetrievalTaskById,
    GetPendingRetrievalTasks,
    GetTasksBySource,
    GetTasksByBatch,
    GetImagesByTask,
    TaskDTO,
    ImageDTO
)


class UoWGetRetrievalTaskByIdHandler(QueryHandler):
    """Handler para obtener una tarea de recuperación por su ID usando UoW"""
    
    def __init__(self, uow: UnitOfWork):
        self.uow = uow

    async def handle(self, query: GetRetrievalTaskById) -> Optional[Dict[str, Any]]:
        async with self.uow:
            # Obtener repositorios
            retrieval_repository = self.uow.repository('retrieval')
            image_repository = self.uow.repository('image')
            
            task = await retrieval_repository.get_by_id(query.task_id)
            if not task:
                return None
                
            # Obtener conteo de imágenes
            images_count = await image_repository.get_images_count_by_task(task.id)
            
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


class UoWGetPendingRetrievalTasksHandler(QueryHandler):
    """Handler para obtener todas las tareas pendientes usando UoW"""
    
    def __init__(self, uow: UnitOfWork):
        self.uow = uow

    async def handle(self, query: GetPendingRetrievalTasks) -> List[Dict[str, Any]]:
        async with self.uow:
            # Obtener repositorios
            retrieval_repository = self.uow.repository('retrieval')
            image_repository = self.uow.repository('image')
            
            tasks = await retrieval_repository.get_pending_tasks()
            result = []
            
            for task in tasks:
                # Obtener conteo de imágenes
                images_count = await image_repository.get_images_count_by_task(task.id)
                
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


class UoWGetTasksBySourceHandler(QueryHandler):
    """Handler para obtener tareas por fuente usando UoW"""
    
    def __init__(self, uow: UnitOfWork):
        self.uow = uow

    async def handle(self, query: GetTasksBySource) -> List[Dict[str, Any]]:
        async with self.uow:
            # Obtener repositorios
            retrieval_repository = self.uow.repository('retrieval')
            image_repository = self.uow.repository('image')
            
            tasks = await retrieval_repository.get_tasks_by_source(query.source_id, query.limit)
            result = []
            
            for task in tasks:
                # Obtener conteo de imágenes
                images_count = await image_repository.get_images_count_by_task(task.id)
                
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


class UoWGetTasksByBatchHandler(QueryHandler):
    """Handler para obtener tareas por lote usando UoW"""
    
    def __init__(self, uow: UnitOfWork):
        self.uow = uow

    async def handle(self, query: GetTasksByBatch) -> List[Dict[str, Any]]:
        async with self.uow:
            # Obtener repositorios
            retrieval_repository = self.uow.repository('retrieval')
            image_repository = self.uow.repository('image')
            
            tasks = await retrieval_repository.get_tasks_by_batch(query.batch_id)
            result = []
            
            for task in tasks:
                # Obtener conteo de imágenes
                images_count = await image_repository.get_images_count_by_task(task.id)
                
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


class UoWGetImagesByTaskHandler(QueryHandler):
    """Handler para obtener imágenes asociadas a una tarea usando UoW"""
    
    def __init__(self, uow: UnitOfWork):
        self.uow = uow

    async def handle(self, query: GetImagesByTask) -> List[Dict[str, Any]]:
        async with self.uow:
            # Obtener repositorio de imágenes
            image_repository = self.uow.repository('image')
            
            images = await image_repository.get_images_by_task(query.task_id)
            
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


# Funciones para ayudar a ejecutar las consultas con UoW
async def uow_get_retrieval_task_by_id(
    handler: UoWGetRetrievalTaskByIdHandler,
    task_id: uuid.UUID
) -> Optional[Dict[str, Any]]:
    """Ejecuta la consulta GetRetrievalTaskById usando UoW"""
    query = GetRetrievalTaskById(task_id=task_id)
    return await handler.handle(query)


async def uow_get_pending_retrieval_tasks(
    handler: UoWGetPendingRetrievalTasksHandler
) -> List[Dict[str, Any]]:
    """Ejecuta la consulta GetPendingRetrievalTasks usando UoW"""
    query = GetPendingRetrievalTasks()
    return await handler.handle(query)


async def uow_get_tasks_by_source(
    handler: UoWGetTasksBySourceHandler,
    source_id: str,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """Ejecuta la consulta GetTasksBySource usando UoW"""
    query = GetTasksBySource(source_id=source_id, limit=limit)
    return await handler.handle(query)


async def uow_get_tasks_by_batch(
    handler: UoWGetTasksByBatchHandler,
    batch_id: str
) -> List[Dict[str, Any]]:
    """Ejecuta la consulta GetTasksByBatch usando UoW"""
    query = GetTasksByBatch(batch_id=batch_id)
    return await handler.handle(query)


async def uow_get_images_by_task(
    handler: UoWGetImagesByTaskHandler,
    task_id: uuid.UUID
) -> List[Dict[str, Any]]:
    """Ejecuta la consulta GetImagesByTask usando UoW"""
    query = GetImagesByTask(task_id=task_id)
    return await handler.handle(query)