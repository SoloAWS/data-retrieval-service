import datetime
from typing import List, Optional
import uuid
from sqlalchemy import select, desc, func
from sqlalchemy.ext.asyncio import AsyncSession

from ...domain.entities import RetrievalTask, ImageData
from ...domain.repositories import RetrievalRepository, ImageRepository
from ...domain.value_objects import (
    SourceMetadata,
    ImageMetadata,
    RetrievalResult,
    RetrievalStatus,
    SourceType,
    RetrievalMethod,
    ImageFormat
)

from .dto import RetrievalTaskDTO, ImageDataDTO


class SQLRetrievalRepository(RetrievalRepository):
    """Implementación de RetrievalRepository con SQLAlchemy"""
    
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, task_id: uuid.UUID) -> Optional[RetrievalTask]:
        """Obtiene una tarea de recuperación por su ID"""
        dto = await self.session.get(RetrievalTaskDTO, task_id)
        if not dto:
            return None
        return await self._dto_to_entity(dto)

    async def save(self, task: RetrievalTask) -> None:
        """Guarda una tarea de recuperación"""
        # Check if the task already exists
        existing_task = await self.session.get(RetrievalTaskDTO, task.id)
        
        if existing_task:
            # If the task exists, update it instead of creating a new one
            await self.update(task)
        else:
            # Otherwise, create a new task
            dto = await self._entity_to_dto(task)
            self.session.add(dto)
            await self.session.commit()

    async def update(self, task: RetrievalTask) -> None:
        """Actualiza una tarea de recuperación existente"""
        # Get existing task
        existing_task = await self.session.get(RetrievalTaskDTO, task.id)
        if not existing_task:
            raise ValueError(f"No se puede actualizar una tarea que no existe: {task.id}")
        
        # Update the existing task with the new values
        existing_task.batch_id = task.batch_id
        existing_task.source_type = task.source_metadata.source_type.value
        existing_task.source_name = task.source_metadata.source_name
        existing_task.source_id = task.source_metadata.source_id
        existing_task.location = task.source_metadata.location
        existing_task.retrieval_method = task.source_metadata.retrieval_method.value
        existing_task.priority = task.priority
        existing_task.storage_path = task.storage_path
        existing_task.status = task.result.status.value if task.result else RetrievalStatus.PENDING.value
        existing_task.message = task.result.message if task.result else None
        existing_task.total_images = task.result.total_images if task.result else 0
        existing_task.successful_images = task.result.successful_images if task.result else 0
        existing_task.failed_images = task.result.failed_images if task.result else 0
        existing_task.details = task.result.details if task.result else None
        existing_task._metadata = task.metadata
        existing_task.updated_at = datetime.datetime.now()
        existing_task.started_at = task.started_at
        existing_task.completed_at = task.completed_at
        
        # No need to add the DTO to the session as it's already tracked
        await self.session.commit()

    async def get_pending_tasks(self) -> List[RetrievalTask]:
        """Obtiene las tareas pendientes ordenadas por prioridad"""
        query = (
            select(RetrievalTaskDTO)
            .filter(RetrievalTaskDTO.status == RetrievalStatus.PENDING.value)
            .order_by(desc(RetrievalTaskDTO.priority))
        )
        result = await self.session.execute(query)
        dtos = result.scalars().all()
        return [await self._dto_to_entity(dto) for dto in dtos]

    async def get_tasks_by_source(self, source_id: str, limit: int = 10) -> List[RetrievalTask]:
        """Obtiene tareas por fuente"""
        query = (
            select(RetrievalTaskDTO)
            .filter(RetrievalTaskDTO.source_id == source_id)
            .order_by(desc(RetrievalTaskDTO.created_at))
            .limit(limit)
        )
        result = await self.session.execute(query)
        dtos = result.scalars().all()
        return [await self._dto_to_entity(dto) for dto in dtos]

    async def get_tasks_by_batch(self, batch_id: str) -> List[RetrievalTask]:
        """Obtiene tareas por lote"""
        query = (
            select(RetrievalTaskDTO)
            .filter(RetrievalTaskDTO.batch_id == batch_id)
            .order_by(desc(RetrievalTaskDTO.created_at))
        )
        result = await self.session.execute(query)
        dtos = result.scalars().all()
        return [await self._dto_to_entity(dto) for dto in dtos]

    async def _dto_to_entity(self, dto: RetrievalTaskDTO) -> RetrievalTask:
        """Convierte un DTO a una entidad de dominio"""
        
        # Convertir los metadatos de la fuente
        source_metadata = SourceMetadata(
            source_type=SourceType(dto.source_type),
            source_name=dto.source_name,
            source_id=dto.source_id,
            location=dto.location,
            retrieval_method=RetrievalMethod(dto.retrieval_method)
        )

        # Convertir el resultado si existe
        result = None
        if dto.status != RetrievalStatus.PENDING.value:
            result = RetrievalResult(
                status=RetrievalStatus(dto.status),
                message=dto.message or "",
                total_images=dto.total_images,
                successful_images=dto.successful_images,
                failed_images=dto.failed_images,
                details=dto.details
            )

        # Crear la entidad de tarea sin imágenes inicialmente
        task = RetrievalTask(
            id=dto.id,
            source_metadata=source_metadata,
            batch_id=dto.batch_id,
            priority=dto.priority,
            storage_path=dto.storage_path,
            result=result,
            started_at=dto.started_at,
            completed_at=dto.completed_at,
            metadata=dto._metadata or {},
            created_at=dto.created_at,
            updated_at=dto.updated_at
        )

        # Obtener y adjuntar imágenes si es necesario
        query = select(ImageDataDTO).filter(ImageDataDTO.task_id == dto.id)
        result = await self.session.execute(query)
        image_dtos = result.scalars().all()

        for image_dto in image_dtos:
            # Crear los metadatos de la imagen
            image_metadata = ImageMetadata(
                format=ImageFormat(image_dto.format),
                modality=image_dto.modality,
                region=image_dto.region,
                size_bytes=image_dto.size_bytes,
                dimensions=image_dto.dimensions
            )

            # Crear la entidad de imagen
            image = ImageData(
                id=image_dto.id,
                metadata=image_metadata,
                filename=image_dto.filename,
                file_path=image_dto.file_path,
                size_bytes=image_dto.size_bytes,
                is_stored=image_dto.is_stored,
                created_at=image_dto.created_at,
                updated_at=image_dto.updated_at
            )
            
            task.images.append(image)

        return task

    async def _entity_to_dto(self, entity: RetrievalTask) -> RetrievalTaskDTO:
        """Convierte una entidad de dominio a un DTO"""
        
        dto = RetrievalTaskDTO(
            id=entity.id,
            batch_id=entity.batch_id,
            source_type=entity.source_metadata.source_type.value,
            source_name=entity.source_metadata.source_name,
            source_id=entity.source_metadata.source_id,
            location=entity.source_metadata.location,
            retrieval_method=entity.source_metadata.retrieval_method.value,
            priority=entity.priority,
            storage_path=entity.storage_path,
            status=entity.result.status.value if entity.result else RetrievalStatus.PENDING.value,
            message=entity.result.message if entity.result else None,
            total_images=entity.result.total_images if entity.result else 0,
            successful_images=entity.result.successful_images if entity.result else 0,
            failed_images=entity.result.failed_images if entity.result else 0,
            details=entity.result.details if entity.result else None,
            _metadata=entity.metadata,
            created_at=entity.created_at,
            updated_at=entity.updated_at,
            started_at=entity.started_at,
            completed_at=entity.completed_at
        )
        
        return dto


class SQLImageRepository(ImageRepository):
    """Implementación de ImageRepository con SQLAlchemy"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        
    async def save(self, image: ImageData, task_id: uuid.UUID) -> None:
        """Guarda una imagen"""
        
        # Verificar si ya existe la imagen
        existing_image = await self.session.get(ImageDataDTO, image.id)
        if existing_image:
            # Si ya existe, actualizar
            await self.update_image(image, task_id)
            return
        
        # Crear DTO de imagen
        image_dto = ImageDataDTO(
            id=image.id,
            task_id=task_id,
            filename=image.filename,
            file_path=image.file_path,
            format=image.metadata.format.value,
            modality=image.metadata.modality,
            region=image.metadata.region,
            size_bytes=image.size_bytes,
            dimensions=image.metadata.dimensions,
            is_stored=image.is_stored,
            created_at=image.created_at,
            updated_at=image.updated_at
        )
        
        self.session.add(image_dto)
        await self.session.commit()
    
    async def update_image(self, image: ImageData, task_id: uuid.UUID) -> None:
        """Actualiza una imagen existente"""
        existing_image = await self.session.get(ImageDataDTO, image.id)
        if not existing_image:
            raise ValueError(f"No se puede actualizar una imagen que no existe: {image.id}")
        
        # Actualizar campos
        existing_image.task_id = task_id
        existing_image.filename = image.filename
        existing_image.file_path = image.file_path
        existing_image.format = image.metadata.format.value
        existing_image.modality = image.metadata.modality
        existing_image.region = image.metadata.region
        existing_image.size_bytes = image.size_bytes
        existing_image.dimensions = image.metadata.dimensions
        existing_image.is_stored = image.is_stored
        existing_image.updated_at = datetime.datetime.now()
        
        await self.session.commit()
    
    async def save_batch(self, images: List[ImageData], task_id: uuid.UUID) -> None:
        """Guarda un lote de imágenes"""
        
        # Para cada imagen, verificar si existe y actualizar o crear nueva
        for image in images:
            await self.save(image, task_id)
    
    async def get_by_id(self, image_id: uuid.UUID) -> Optional[ImageData]:
        """Obtiene una imagen por su ID"""
        
        dto = await self.session.get(ImageDataDTO, image_id)
        if not dto:
            return None
            
        # Crear metadatos
        metadata = ImageMetadata(
            format=ImageFormat(dto.format),
            modality=dto.modality,
            region=dto.region,
            size_bytes=dto.size_bytes,
            dimensions=dto.dimensions
        )
        
        # Crear entidad
        return ImageData(
            id=dto.id,
            metadata=metadata,
            filename=dto.filename,
            file_path=dto.file_path,
            size_bytes=dto.size_bytes,
            is_stored=dto.is_stored,
            created_at=dto.created_at,
            updated_at=dto.updated_at
        )
    
    async def get_images_by_task(self, task_id: uuid.UUID) -> List[ImageData]:
        """Obtiene todas las imágenes asociadas a una tarea"""
        
        query = select(ImageDataDTO).filter(ImageDataDTO.task_id == task_id)
        result = await self.session.execute(query)
        dtos = result.scalars().all()
        
        images = []
        for dto in dtos:
            # Crear metadatos
            metadata = ImageMetadata(
                format=ImageFormat(dto.format),
                modality=dto.modality,
                region=dto.region,
                size_bytes=dto.size_bytes,
                dimensions=dto.dimensions
            )
            
            # Crear entidad
            image = ImageData(
                id=dto.id,
                metadata=metadata,
                filename=dto.filename,
                file_path=dto.file_path,
                size_bytes=dto.size_bytes,
                is_stored=dto.is_stored,
                created_at=dto.created_at,
                updated_at=dto.updated_at
            )
            
            images.append(image)
            
        return images
    
    async def get_images_count_by_task(self, task_id: uuid.UUID) -> int:
        """Obtiene el número de imágenes asociadas a una tarea"""
        
        query = select(func.count()).select_from(ImageDataDTO).filter(ImageDataDTO.task_id == task_id)
        result = await self.session.execute(query)
        return result.scalar_one()
    
    async def update_image_status(self, image_id: uuid.UUID, is_stored: bool) -> None:
        """Actualiza el estado de almacenamiento de una imagen"""
        
        dto = await self.session.get(ImageDataDTO, image_id)
        if not dto:
            return
            
        dto.is_stored = is_stored
        dto.updated_at = datetime.datetime.now()
        await self.session.commit()