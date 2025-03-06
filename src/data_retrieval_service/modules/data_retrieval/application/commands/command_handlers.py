import logging
import os
import uuid
from typing import Dict, Any, Optional

from .....seedwork.infrastructure.uow import UnitOfWork
from ...domain.value_objects import SourceType, RetrievalMethod, ImageFormat
from ...infrastructure.messaging.pulsar_publisher import PulsarPublisher
from ...application.commands.uow_commands import (
    UoWCreateRetrievalTaskHandler,
    UoWStartRetrievalTaskHandler,
    UoWStoreImageHandler,
    uow_create_retrieval_task,
    uow_start_retrieval_task,
    uow_store_image
)

from .....config.settings import get_settings

logger = logging.getLogger(__name__)

# Manejadores de comandos recibidos via Pulsar

async def handle_create_retrieval_task(
    command_data: Dict[str, Any],
    uow: UnitOfWork,
    publisher: PulsarPublisher,
    correlation_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Manejador para el comando CreateRetrievalTask recibido via Pulsar.
    
    Args:
        command_data: Datos del comando
        uow: Unidad de trabajo para transacciones
        publisher: Publicador de eventos
        correlation_id: ID de correlación opcional
        
    Returns:
        Dict: Resultado de la operación
    """
    logger.info(f"Processing CreateRetrievalTask command: {command_data}")
    
    try:
        # Crear handler con UoW
        handler = UoWCreateRetrievalTaskHandler(uow, publisher)
        
        # Extraer datos del comando
        source_type_str = command_data.get('source_type')
        source_name = command_data.get('source_name')
        source_id = command_data.get('source_id')
        location = command_data.get('location')
        retrieval_method_str = command_data.get('retrieval_method')
        batch_id = command_data.get('batch_id')
        priority = command_data.get('priority', 0)
        metadata = command_data.get('metadata')
        
        # Validar datos requeridos
        if not all([source_type_str, source_name, source_id, location, retrieval_method_str, batch_id]):
            raise ValueError("Missing required command data fields")
        
        # Convertir enums
        try:
            source_type = SourceType[source_type_str]
            retrieval_method = RetrievalMethod[retrieval_method_str]
        except KeyError as e:
            raise ValueError(f"Invalid enum value: {str(e)}")
        
        # Crear ruta de almacenamiento
        settings = get_settings()
        storage_path = os.path.join(
            settings.image_storage_path,
            source_type.value.lower(),
            batch_id
        )
        
        # Ejecutar comando usando UoW
        result = await uow_create_retrieval_task(
            handler=handler,
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
        
        # Añadir correlation_id a la respuesta
        if correlation_id:
            result["correlation_id"] = correlation_id
        
        return result
    except Exception as e:
        logger.error(f"Error handling CreateRetrievalTask command: {str(e)}")
        raise

async def handle_start_retrieval_task(
    command_data: Dict[str, Any],
    uow: UnitOfWork,
    publisher: PulsarPublisher,
    correlation_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Manejador para el comando StartRetrievalTask recibido via Pulsar.
    
    Args:
        command_data: Datos del comando
        uow: Unidad de trabajo para transacciones
        publisher: Publicador de eventos
        correlation_id: ID de correlación opcional
        
    Returns:
        Dict: Resultado de la operación
    """
    logger.info(f"Processing StartRetrievalTask command: {command_data}")
    
    try:
        # Crear handler con UoW
        handler = UoWStartRetrievalTaskHandler(uow, publisher)
        
        # Extraer datos del comando
        task_id_str = command_data.get('task_id')
        
        # Validar datos requeridos
        if not task_id_str:
            raise ValueError("Missing required command data fields: task_id")
        
        # Convertir ID a UUID
        try:
            task_id = uuid.UUID(task_id_str)
        except ValueError:
            raise ValueError(f"Invalid task_id format: {task_id_str}")
        
        # Ejecutar comando usando UoW
        result = await uow_start_retrieval_task(
            handler=handler,
            task_id=task_id
        )
        
        # Añadir correlation_id a la respuesta
        if correlation_id:
            result["correlation_id"] = correlation_id
        
        return result
    except Exception as e:
        logger.error(f"Error handling StartRetrievalTask command: {str(e)}")
        raise

async def handle_upload_image(
    command_data: Dict[str, Any],
    uow: UnitOfWork,
    publisher: PulsarPublisher,
    correlation_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Manejador para el comando UploadImage recibido via Pulsar.
    Nota: Este comando normalmente no se enviaría por Pulsar debido a los archivos binarios.
    Existe principalmente para pruebas o para flujos especiales.
    
    Args:
        command_data: Datos del comando
        uow: Unidad de trabajo para transacciones
        publisher: Publicador de eventos
        correlation_id: ID de correlación opcional
        
    Returns:
        Dict: Resultado de la operación
    """
    logger.warning("UploadImage command received via Pulsar, which is not recommended for binary files")
    logger.info(f"Processing UploadImage command for task: {command_data.get('task_id')}")
    
    try:
        # Crear handler con UoW
        handler = UoWStoreImageHandler(uow, publisher)
        
        # Extraer datos del comando
        task_id_str = command_data.get('task_id')
        file_content_b64 = command_data.get('file_content')
        filename = command_data.get('filename')
        format_str = command_data.get('format')
        modality = command_data.get('modality')
        region = command_data.get('region')
        dimensions = command_data.get('dimensions')
        
        # Validar datos requeridos
        if not all([task_id_str, file_content_b64, filename, format_str, modality, region]):
            raise ValueError("Missing required command data fields")
        
        # Convertir UUID y enums
        try:
            task_id = uuid.UUID(task_id_str)
            format_enum = ImageFormat[format_str]
        except KeyError as e:
            raise ValueError(f"Invalid enum value: {str(e)}")
        except ValueError:
            raise ValueError(f"Invalid task_id format: {task_id_str}")
        
        # Decodificar contenido del archivo (asumiendo base64)
        import base64
        try:
            file_content = base64.b64decode(file_content_b64)
        except:
            raise ValueError("Invalid file_content format, expected base64")
        
        # Ejecutar comando usando UoW
        result = await uow_store_image(
            handler=handler,
            task_id=task_id,
            file_content=file_content,
            filename=filename,
            format=format_enum,
            modality=modality,
            region=region,
            dimensions=dimensions
        )
        
        # Añadir correlation_id a la respuesta
        if correlation_id:
            result["correlation_id"] = correlation_id
        
        return result
    except Exception as e:
        logger.error(f"Error handling UploadImage command: {str(e)}")
        raise

# Mapa de comandos a manejadores
command_handlers = {
    "CreateRetrievalTask": handle_create_retrieval_task,
    "StartRetrievalTask": handle_start_retrieval_task,
    "UploadImage": handle_upload_image,
}