from dataclasses import dataclass
from typing import List, Dict, Optional, Any
import uuid
import os
import shutil
from datetime import datetime

from .....seedwork.application.commands import Command, CommandHandler
from .....seedwork.infrastructure.uow import UnitOfWork
from ...domain.repositories import RetrievalRepository, ImageRepository
from ...domain.entities import RetrievalTask, ImageData
from ...domain.value_objects import (
    SourceMetadata, 
    ImageMetadata, 
    SourceType, 
    RetrievalMethod, 
    ImageFormat,
    RetrievalStatus
)
from ...infrastructure.messaging.pulsar_publisher import PulsarPublisher

# Reutilizamos las definiciones de comandos existentes
from .commands import (
    CreateRetrievalTask,
    StartRetrievalTask,
    CompleteRetrievalTask,
    FailRetrievalTask,
    StoreImage,
    StoreImageBatch
)


class UoWCreateRetrievalTaskHandler(CommandHandler):
    """Manejador para el comando CreateRetrievalTask usando UoW"""
    
    def __init__(self, uow: UnitOfWork, publisher: PulsarPublisher):
        self.uow = uow
        self.publisher = publisher

    async def handle(self, command: CreateRetrievalTask) -> Dict[str, Any]:
        async with self.uow:
            try:
                # Obtener repositorio de tareas
                retrieval_repository = self.uow.repository('retrieval')
                
                # Crear metadata de origen
                source_metadata = SourceMetadata(
                    source_type=command.source_type,
                    source_name=command.source_name,
                    source_id=command.source_id,
                    location=command.location,
                    retrieval_method=command.retrieval_method
                )

                # Crear tarea de recuperación
                task = RetrievalTask(
                    source_metadata=source_metadata,
                    batch_id=command.batch_id,
                    priority=command.priority,
                    storage_path=command.storage_path,
                    metadata=command.metadata or {}
                )

                # Guardar la tarea en el repositorio
                await retrieval_repository.save(task)
                
                # Confirmar la transacción
                await self.uow.commit()

                # Retornar información de la tarea creada
                return {
                    "task_id": str(task.id),
                    "batch_id": task.batch_id,
                    "source": task.source_metadata.source_name,
                    "status": "PENDING"
                }
            except Exception as e:
                # No es necesario hacer rollback explícito ya que el context manager lo hará
                raise e


class UoWStartRetrievalTaskHandler(CommandHandler):
    """Manejador para el comando StartRetrievalTask usando UoW"""
    
    def __init__(self, uow: UnitOfWork, publisher: PulsarPublisher):
        self.uow = uow
        self.publisher = publisher

    async def handle(self, command: StartRetrievalTask) -> Dict[str, Any]:
        async with self.uow:
            try:
                # Obtener repositorio de tareas
                retrieval_repository = self.uow.repository('retrieval')
                
                # Obtener la tarea del repositorio
                task = await retrieval_repository.get_by_id(command.task_id)
                if not task:
                    raise ValueError(f"No se encontró la tarea con ID: {command.task_id}")

                # Asegurarnos que existe el directorio de almacenamiento
                os.makedirs(task.storage_path, exist_ok=True)

                # Iniciar la tarea
                event = task.start_retrieval()
                
                # Publicar evento de inicio
                await self.publisher.publish_event(event)
                
                # Actualizar la tarea en el repositorio
                await retrieval_repository.update(task)
                
                # Confirmar la transacción
                await self.uow.commit()

                return {
                    "task_id": str(task.id),
                    "batch_id": task.batch_id,
                    "source": task.source_metadata.source_name,
                    "status": "IN_PROGRESS",
                    "started_at": task.started_at.isoformat() if task.started_at else None
                }
            except Exception as e:
                # No es necesario hacer rollback explícito ya que el context manager lo hará
                raise e


class UoWCompleteRetrievalTaskHandler(CommandHandler):
    """Manejador para el comando CompleteRetrievalTask usando UoW"""
    
    def __init__(self, uow: UnitOfWork, publisher: PulsarPublisher):
        self.uow = uow
        self.publisher = publisher

    async def handle(self, command: CompleteRetrievalTask) -> Dict[str, Any]:
        async with self.uow:
            try:
                # Obtener repositorio de tareas
                retrieval_repository = self.uow.repository('retrieval')
                
                # Obtener la tarea del repositorio
                task = await retrieval_repository.get_by_id(command.task_id)
                if not task:
                    raise ValueError(f"No se encontró la tarea con ID: {command.task_id}")

                # Completar la tarea
                event = task.complete_retrieval(
                    successful_images=command.successful_images,
                    failed_images=command.failed_images,
                    details=command.details
                )
                
                # Publicar evento de completado
                await self.publisher.publish_event(event)
                
                # Actualizar la tarea en el repositorio
                await retrieval_repository.update(task)
                
                # Confirmar la transacción
                await self.uow.commit()

                return {
                    "task_id": str(task.id),
                    "batch_id": task.batch_id,
                    "source": task.source_metadata.source_name,
                    "status": "COMPLETED",
                    "successful_images": command.successful_images,
                    "failed_images": command.failed_images,
                    "total_images": command.successful_images + command.failed_images,
                    "completed_at": task.completed_at.isoformat() if task.completed_at else None
                }
            except Exception as e:
                # No es necesario hacer rollback explícito ya que el context manager lo hará
                raise e


class UoWFailRetrievalTaskHandler(CommandHandler):
    """Manejador para el comando FailRetrievalTask usando UoW"""
    
    def __init__(self, uow: UnitOfWork, publisher: PulsarPublisher):
        self.uow = uow
        self.publisher = publisher

    async def handle(self, command: FailRetrievalTask) -> Dict[str, Any]:
        async with self.uow:
            try:
                # Obtener repositorio de tareas
                retrieval_repository = self.uow.repository('retrieval')
                
                # Obtener la tarea del repositorio
                task = await retrieval_repository.get_by_id(command.task_id)
                if not task:
                    raise ValueError(f"No se encontró la tarea con ID: {command.task_id}")

                # Marcar la tarea como fallida
                event = task.fail_retrieval(
                    error_message=command.error_message,
                    details=command.details
                )
                
                # Publicar evento de falla
                await self.publisher.publish_event(event)
                
                # Actualizar la tarea en el repositorio
                await retrieval_repository.update(task)
                
                # Confirmar la transacción
                await self.uow.commit()

                return {
                    "task_id": str(task.id),
                    "batch_id": task.batch_id,
                    "source": task.source_metadata.source_name,
                    "status": "FAILED",
                    "error_message": command.error_message,
                    "completed_at": task.completed_at.isoformat() if task.completed_at else None
                }
            except Exception as e:
                # No es necesario hacer rollback explícito ya que el context manager lo hará
                raise e


class UoWStoreImageHandler(CommandHandler):
    """Manejador para el comando StoreImage usando UoW"""
    
    def __init__(self, uow: UnitOfWork, publisher: PulsarPublisher):
        self.uow = uow
        self.publisher = publisher

    async def handle(self, command: StoreImage) -> Dict[str, Any]:
        async with self.uow:
            try:
                # Obtener repositorios
                retrieval_repository = self.uow.repository('retrieval')
                image_repository = self.uow.repository('image')
                
                # Obtener la tarea
                task = await retrieval_repository.get_by_id(command.task_id)
                if not task:
                    raise ValueError(f"No se encontró la tarea con ID: {command.task_id}")

                # Asegurarse que existe el directorio de la tarea
                task_dir = os.path.join(task.storage_path, str(task.id))
                os.makedirs(task_dir, exist_ok=True)
                
                # Generar ruta completa para la imagen
                filename = command.filename
                file_path = os.path.join(task_dir, filename)
                
                # Escribir la imagen en el disco
                with open(file_path, 'wb') as f:
                    f.write(command.file_content)
                
                # Calcular tamaño del archivo
                size_bytes = os.path.getsize(file_path)
                
                # Crear metadatos de imagen
                image_metadata = ImageMetadata(
                    format=command.format,
                    modality=command.modality,
                    region=command.region,
                    size_bytes=size_bytes,
                    dimensions=command.dimensions
                )

                # Crear entidad de imagen
                image = ImageData(
                    metadata=image_metadata,
                    filename=filename,
                    file_path=file_path,
                    size_bytes=size_bytes,
                    is_stored=True
                )

                # Añadir la imagen a la tarea
                task.add_image(image)
                
                # Guardar la imagen en el repositorio
                await image_repository.save(image, task.id)
                            
                # Notificar que la imagen está lista para anonimización
                task.notify_images_retrieved([image])

                # Publicar eventos
                for event in task.events:
                    await self.publisher.publish_event(event)

                # Actualizar la tarea en el repositorio
                await retrieval_repository.update(task)
                
                # Confirmar la transacción
                await self.uow.commit()

                return {
                    "task_id": str(task.id),
                    "image_id": str(image.id),
                    "filename": image.filename,
                    "file_path": image.file_path,
                    "modality": image.metadata.modality,
                    "region": image.metadata.region,
                    "size_bytes": image.size_bytes
                }
            except Exception as e:
                # No es necesario hacer rollback explícito ya que el context manager lo hará
                raise e


class UoWStoreImageBatchHandler(CommandHandler):
    """Manejador para el comando StoreImageBatch usando UoW"""
    
    def __init__(self, uow: UnitOfWork, publisher: PulsarPublisher):
        self.uow = uow
        self.publisher = publisher

    async def handle(self, command: StoreImageBatch) -> Dict[str, Any]:
        async with self.uow:
            try:
                # Obtener repositorios
                retrieval_repository = self.uow.repository('retrieval')
                image_repository = self.uow.repository('image')
                
                # Obtener la tarea
                task = await retrieval_repository.get_by_id(command.task_id)
                if not task:
                    raise ValueError(f"No se encontró la tarea con ID: {command.task_id}")

                # Asegurarse que existe el directorio de la tarea
                task_dir = os.path.join(task.storage_path, str(task.id))
                os.makedirs(task_dir, exist_ok=True)
                
                # Procesar lote de imágenes
                stored_images = []
                for img_data in command.images:
                    # Extraer datos de la imagen
                    filename = img_data['filename']
                    file_content = img_data['file_content']
                    format_str = img_data['format']
                    modality = img_data['modality']
                    region = img_data['region']
                    dimensions = img_data.get('dimensions')
                    
                    # Generar ruta completa para la imagen
                    file_path = os.path.join(task_dir, filename)
                    
                    # Escribir la imagen en el disco
                    with open(file_path, 'wb') as f:
                        f.write(file_content)
                    
                    # Calcular tamaño del archivo
                    size_bytes = os.path.getsize(file_path)
                    
                    # Crear metadatos de imagen
                    image_metadata = ImageMetadata(
                        format=ImageFormat(format_str),
                        modality=modality,
                        region=region,
                        size_bytes=size_bytes,
                        dimensions=dimensions
                    )

                    # Crear entidad de imagen
                    image = ImageData(
                        metadata=image_metadata,
                        filename=filename,
                        file_path=file_path,
                        size_bytes=size_bytes,
                        is_stored=True
                    )

                    # Añadir la imagen a la tarea y a nuestra lista local
                    task.add_image(image)
                    stored_images.append(image)
                
                # Guardar lote de imágenes en el repositorio
                await image_repository.save_batch(stored_images, task.id)

                # Notificar que las imágenes están listas para anonimización
                task.notify_images_retrieved(stored_images)
                
                # Publicar eventos
                for event in task.events:
                    await self.publisher.publish_event(event)

                # Actualizar la tarea en el repositorio
                await retrieval_repository.update(task)
                
                # Confirmar la transacción
                await self.uow.commit()

                return {
                    "task_id": str(task.id),
                    "images_count": len(stored_images),
                    "total_size_bytes": sum(img.size_bytes for img in stored_images),
                    "images": [
                        {
                            "image_id": str(img.id),
                            "filename": img.filename,
                            "modality": img.metadata.modality,
                            "region": img.metadata.region,
                            "size_bytes": img.size_bytes
                        }
                        for img in stored_images
                    ]
                }
            except Exception as e:
                # No es necesario hacer rollback explícito ya que el context manager lo hará
                raise e


# Funciones para ayudar a ejecutar los comandos con UoW
async def uow_create_retrieval_task(
    handler: UoWCreateRetrievalTaskHandler,
    source_type: SourceType,
    source_name: str,
    source_id: str,
    location: str,
    retrieval_method: RetrievalMethod,
    batch_id: str,
    storage_path: str,
    priority: int = 0,
    metadata: Dict = None
) -> Dict[str, Any]:
    """Ejecuta el comando CreateRetrievalTask con UoW"""
    command = CreateRetrievalTask(
        source_type=source_type,
        source_name=source_name,
        source_id=source_id,
        location=location,
        retrieval_method=retrieval_method,
        batch_id=batch_id,
        storage_path=storage_path,
        priority=priority,
        metadata=metadata
    )
    return await handler.handle(command)


async def uow_start_retrieval_task(
    handler: UoWStartRetrievalTaskHandler,
    task_id: uuid.UUID
) -> Dict[str, Any]:
    """Ejecuta el comando StartRetrievalTask con UoW"""
    command = StartRetrievalTask(task_id=task_id)
    return await handler.handle(command)


async def uow_complete_retrieval_task(
    handler: UoWCompleteRetrievalTaskHandler,
    task_id: uuid.UUID,
    successful_images: int,
    failed_images: int,
    details: Optional[List[Dict]] = None
) -> Dict[str, Any]:
    """Ejecuta el comando CompleteRetrievalTask con UoW"""
    command = CompleteRetrievalTask(
        task_id=task_id,
        successful_images=successful_images,
        failed_images=failed_images,
        details=details
    )
    return await handler.handle(command)


async def uow_fail_retrieval_task(
    handler: UoWFailRetrievalTaskHandler,
    task_id: uuid.UUID,
    error_message: str,
    details: Optional[List[Dict]] = None
) -> Dict[str, Any]:
    """Ejecuta el comando FailRetrievalTask con UoW"""
    command = FailRetrievalTask(
        task_id=task_id,
        error_message=error_message,
        details=details
    )
    return await handler.handle(command)


async def uow_store_image(
    handler: UoWStoreImageHandler,
    task_id: uuid.UUID,
    file_content: bytes,
    filename: str,
    format: ImageFormat,
    modality: str,
    region: str,
    dimensions: Optional[str] = None
) -> Dict[str, Any]:
    """Ejecuta el comando StoreImage con UoW"""
    command = StoreImage(
        task_id=task_id,
        file_content=file_content,
        filename=filename,
        format=format,
        modality=modality,
        region=region,
        dimensions=dimensions
    )
    return await handler.handle(command)


async def uow_store_image_batch(
    handler: UoWStoreImageBatchHandler,
    task_id: uuid.UUID,
    images: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Ejecuta el comando StoreImageBatch con UoW"""
    command = StoreImageBatch(
        task_id=task_id,
        images=images
    )
    return await handler.handle(command)