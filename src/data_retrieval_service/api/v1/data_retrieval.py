from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, UploadFile, File, Depends, Form, BackgroundTasks
from fastapi.responses import JSONResponse
import uuid
from pydantic import BaseModel, Field, validator
from enum import Enum
import logging
import os
import traceback

from ...modules.data_retrieval.application.commands.uow_commands import (
    UoWCreateRetrievalTaskHandler,
    UoWStartRetrievalTaskHandler,
    UoWCompleteRetrievalTaskHandler,
    UoWFailRetrievalTaskHandler,
    UoWStoreImageHandler,
    UoWStoreImageBatchHandler,
    uow_create_retrieval_task,
    uow_start_retrieval_task,
    uow_complete_retrieval_task,
    uow_fail_retrieval_task,
    uow_store_image,
    uow_store_image_batch
)

from ...modules.data_retrieval.application.queries.uow_queries import (
    UoWGetRetrievalTaskByIdHandler,
    UoWGetPendingRetrievalTasksHandler,
    UoWGetTasksBySourceHandler,
    UoWGetTasksByBatchHandler,
    UoWGetImagesByTaskHandler,
    uow_get_retrieval_task_by_id,
    uow_get_pending_retrieval_tasks,
    uow_get_tasks_by_source,
    uow_get_tasks_by_batch,
    uow_get_images_by_task
)

from ...modules.data_retrieval.domain.value_objects import SourceType, RetrievalMethod, ImageFormat
from ...config.dependencies import get_publisher, get_unit_of_work
from ...config.settings import get_settings

# Configurar logging
logger = logging.getLogger(__name__)

# Crear router
router = APIRouter()


# Pydantic models for API requests
class SourceTypeEnum(str, Enum):
    HOSPITAL = "HOSPITAL"
    LABORATORY = "LABORATORY"
    CLINIC = "CLINIC"
    RESEARCH_CENTER = "RESEARCH_CENTER"


class RetrievalMethodEnum(str, Enum):
    SFTP = "SFTP"
    API = "API"
    DIRECT_UPLOAD = "DIRECT_UPLOAD"
    CLOUD_STORAGE = "CLOUD_STORAGE"


class ImageFormatEnum(str, Enum):
    DICOM = "DICOM"
    JPEG = "JPEG"
    PNG = "PNG"
    TIFF = "TIFF"
    RAW = "RAW"


class CreateTaskRequest(BaseModel):
    source_type: SourceTypeEnum
    source_name: str
    source_id: str
    location: str
    retrieval_method: RetrievalMethodEnum
    batch_id: str
    priority: int = 0
    metadata: Optional[Dict[str, Any]] = None


class CompleteTaskRequest(BaseModel):
    successful_images: int
    failed_images: int
    details: Optional[List[Dict[str, Any]]] = None


class FailTaskRequest(BaseModel):
    error_message: str
    details: Optional[List[Dict[str, Any]]] = None


class ImageMetadataRequest(BaseModel):
    format: ImageFormatEnum
    modality: str
    region: str
    dimensions: Optional[str] = None


# Endpoints
@router.post("/tasks", status_code=201, response_model=Dict[str, Any])
async def api_create_task(
    request: CreateTaskRequest,
    background_tasks: BackgroundTasks,
    publisher = Depends(get_publisher),
    uow = Depends(get_unit_of_work)
):
    """Crea una nueva tarea de recuperación de imágenes"""
    try:
        # Crear handler con UoW
        handler = UoWCreateRetrievalTaskHandler(
            uow,
            publisher
        )
        
        # Crear ruta de almacenamiento
        settings = get_settings()
        storage_path = os.path.join(
            settings.image_storage_path,
            request.source_type.lower(),
            request.batch_id
        )
        
        # Ejecutar comando usando UoW
        result = await uow_create_retrieval_task(
            handler=handler,
            source_type=SourceType[request.source_type.value],
            source_name=request.source_name,
            source_id=request.source_id,
            location=request.location,
            retrieval_method=RetrievalMethod[request.retrieval_method.value],
            batch_id=request.batch_id,
            storage_path=storage_path,
            priority=request.priority,
            metadata=request.metadata
        )
        
        return result
    except Exception as e:
        logger.error(f"Error al crear tarea: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}", response_model=Dict[str, Any])
async def api_get_task(
    task_id: str,
    uow = Depends(get_unit_of_work)
):
    """Obtiene información de una tarea específica"""
    try:
        # Crear handler con UoW
        handler = UoWGetRetrievalTaskByIdHandler(uow)
        
        # Ejecutar consulta usando UoW
        result = await uow_get_retrieval_task_by_id(
            handler=handler,
            task_id=uuid.UUID(task_id)
        )
        
        if not result:
            raise HTTPException(status_code=404, detail=f"Task with ID {task_id} not found")
            
        return result
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid task ID: {task_id}")
    except Exception as e:
        logger.error(f"Error al obtener tarea: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/start", response_model=Dict[str, Any])
async def api_start_task(
    task_id: str,
    publisher = Depends(get_publisher),
    uow = Depends(get_unit_of_work)
):
    """Inicia una tarea de recuperación"""
    try:
        # Crear handler con UoW
        handler = UoWStartRetrievalTaskHandler(
            uow,
            publisher
        )
        
        # Ejecutar comando usando UoW
        result = await uow_start_retrieval_task(
            handler=handler,
            task_id=uuid.UUID(task_id)
        )
        
        return result
    except ValueError as e:
        if "No se encontró la tarea" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al iniciar tarea: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/complete", response_model=Dict[str, Any])
async def api_complete_task(
    task_id: str,
    request: CompleteTaskRequest,
    publisher = Depends(get_publisher),
    uow = Depends(get_unit_of_work)
):
    """Marca una tarea como completada"""
    try:
        # Crear handler con UoW
        handler = UoWCompleteRetrievalTaskHandler(
            uow,
            publisher
        )
        
        # Ejecutar comando usando UoW
        result = await uow_complete_retrieval_task(
            handler=handler,
            task_id=uuid.UUID(task_id),
            successful_images=request.successful_images,
            failed_images=request.failed_images,
            details=request.details
        )
        
        return result
    except ValueError as e:
        if "No se encontró la tarea" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al completar tarea: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/fail", response_model=Dict[str, Any])
async def api_fail_task(
    task_id: str,
    request: FailTaskRequest,
    publisher = Depends(get_publisher),
    uow = Depends(get_unit_of_work)
):
    """Marca una tarea como fallida"""
    try:
        # Crear handler con UoW
        handler = UoWFailRetrievalTaskHandler(
            uow,
            publisher
        )
        
        # Ejecutar comando usando UoW
        result = await uow_fail_retrieval_task(
            handler=handler,
            task_id=uuid.UUID(task_id),
            error_message=request.error_message,
            details=request.details
        )
        
        return result
    except ValueError as e:
        if "No se encontró la tarea" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al marcar tarea como fallida: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/images", response_model=Dict[str, Any])
async def api_upload_image(
    task_id: str,
    file: UploadFile = File(...),
    format: ImageFormatEnum = Form(...),
    modality: str = Form(...),
    region: str = Form(...),
    dimensions: Optional[str] = Form(None),
    publisher = Depends(get_publisher),
    uow = Depends(get_unit_of_work)
):
    """Sube una imagen para una tarea específica"""
    try:
        # Leer el contenido del archivo
        file_content = await file.read()
        
        # Crear handler con UoW
        handler = UoWStoreImageHandler(
            uow,
            publisher
        )
        
        # Ejecutar comando usando UoW
        result = await uow_store_image(
            handler=handler,
            task_id=uuid.UUID(task_id),
            file_content=file_content,
            filename=file.filename,
            format=ImageFormat[format.value],
            modality=modality,
            region=region,
            dimensions=dimensions
        )
                
        return result
    except ValueError as e:
        if "No se encontró la tarea" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al subir imagen: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks", response_model=List[Dict[str, Any]])
async def api_get_tasks(
    source_id: Optional[str] = None,
    batch_id: Optional[str] = None,
    pending_only: bool = False,
    limit: int = 10,
    uow = Depends(get_unit_of_work)
):
    """Obtiene tareas de recuperación con filtros opcionales"""
    try:
        if pending_only:
            # Obtener tareas pendientes
            handler = UoWGetPendingRetrievalTasksHandler(uow)
            return await uow_get_pending_retrieval_tasks(handler)
        elif source_id:
            # Obtener tareas por fuente
            handler = UoWGetTasksBySourceHandler(uow)
            return await uow_get_tasks_by_source(handler, source_id, limit)
        elif batch_id:
            # Obtener tareas por lote
            handler = UoWGetTasksByBatchHandler(uow)
            return await uow_get_tasks_by_batch(handler, batch_id)
        else:
            # Se requiere al menos un filtro
            raise HTTPException(
                status_code=400, 
                detail="Se requiere al menos un filtro: source_id, batch_id o pending_only"
            )
    except Exception as e:
        logger.error(f"Error al obtener tareas: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/images", response_model=List[Dict[str, Any]])
async def api_get_task_images(
    task_id: str,
    uow = Depends(get_unit_of_work)
):
    """Obtiene las imágenes asociadas a una tarea específica"""
    try:
        # Crear handler con UoW
        handler = UoWGetImagesByTaskHandler(uow)
        
        # Ejecutar consulta usando UoW
        result = await uow_get_images_by_task(
            handler=handler,
            task_id=uuid.UUID(task_id)
        )
        
        return result
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid task ID: {task_id}")
    except Exception as e:
        logger.error(f"Error al obtener imágenes: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/images/batch", response_model=Dict[str, Any])
async def api_upload_image_batch(
    task_id: str,
    files: List[UploadFile] = File(...),
    metadata: List[Dict[str, Any]] = None,
    publisher = Depends(get_publisher),
    uow = Depends(get_unit_of_work)
):
    """Sube un lote de imágenes para una tarea específica"""
    
    if not metadata or len(metadata) != len(files):
        raise HTTPException(
            status_code=400, 
            detail="Debe proporcionar metadatos para cada imagen y la misma cantidad de archivos y metadatos"
        )
    
    try:
        # Preparar datos de las imágenes
        images_data = []
        for i, file in enumerate(files):
            # Leer el contenido del archivo
            file_content = await file.read()
            
            # Obtener metadatos de la imagen
            img_metadata = metadata[i]
            format_str = img_metadata.get("format")
            modality = img_metadata.get("modality")
            region = img_metadata.get("region")
            dimensions = img_metadata.get("dimensions")
            
            # Validar metadatos requeridos
            if not all([format_str, modality, region]):
                raise ValueError(f"Metadatos incompletos para la imagen {i+1}: {file.filename}")
            
            # Agregar datos de la imagen
            images_data.append({
                "file_content": file_content,
                "filename": file.filename,
                "format": format_str,
                "modality": modality,
                "region": region,
                "dimensions": dimensions
            })
        
        # Crear handler con UoW
        handler = UoWStoreImageBatchHandler(
            uow,
            publisher
        )
        
        # Ejecutar comando usando UoW
        result = await uow_store_image_batch(
            handler=handler,
            task_id=uuid.UUID(task_id),
            images=images_data
        )
        
        return result
    except ValueError as e:
        if "No se encontró la tarea" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error al subir lote de imágenes: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))