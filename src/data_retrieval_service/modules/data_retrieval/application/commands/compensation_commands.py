# src/data_retrieval_service/modules/data_retrieval/application/commands/compensation_commands.py
from dataclasses import dataclass
import logging
import os
import uuid
import shutil
from typing import Dict, Any, Optional

from .....seedwork.application.commands import Command, CommandHandler
from .....seedwork.infrastructure.uow import UnitOfWork
from ...domain.repositories import RetrievalRepository, ImageRepository
from ...domain.value_objects import RetrievalStatus
from ...infrastructure.messaging.pulsar_publisher import PulsarPublisher
from ...domain.events import ImageDeletionCompleted, ImageDeletionFailed

logger = logging.getLogger(__name__)

@dataclass
class DeleteRetrievedImageCommand(Command):
    """Comando para eliminar una imagen recuperada como compensación"""
    image_id: uuid.UUID
    task_id: uuid.UUID
    reason: str = "Compensación de saga"

class DeleteRetrievedImageHandler(CommandHandler):
    """Manejador para el comando DeleteRetrievedImage"""
    
    def __init__(self, uow: UnitOfWork, publisher: PulsarPublisher):
        self.uow = uow
        self.publisher = publisher

    async def handle(self, command: DeleteRetrievedImageCommand) -> Dict[str, Any]:
        async with self.uow:
            try:
                # Obtener repositorios
                image_repository = self.uow.repository('image')
                task_repository = self.uow.repository('retrieval')
                
                # Obtener la imagen
                image = await image_repository.get_by_id(command.image_id)
                if not image:
                    raise ValueError(f"No se encontró la imagen con ID: {command.image_id}")
                
                # Obtener la tarea
                task = await task_repository.get_by_id(command.task_id)
                if not task:
                    raise ValueError(f"No se encontró la tarea con ID: {command.task_id}")
                
                # 1. Eliminar el archivo físico
                file_path = image.file_path
                if file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        logger.info(f"Archivo eliminado: {file_path}")
                    except Exception as e:
                        logger.error(f"Error al eliminar archivo {file_path}: {str(e)}")
                        raise
                
                # 2. Actualizar el estado de la imagen a "eliminada"
                await image_repository.update_image_status(image.id, is_stored=False)
                
                # 3. Si no quedan más imágenes, actualizar el estado de la tarea
                images_count = await image_repository.get_images_count_by_task(task.id)
                if images_count <= 1:  # <= 1 porque aún no hemos eliminado realmente la imagen de la BD
                    # Marcar la tarea como fallida por compensación
                    fail_event = task.fail_retrieval(
                        error_message=f"Tarea fallida por compensación de saga: {command.reason}"
                    )
                    await task_repository.update(task)
                    await self.publisher.publish_event(fail_event)
                    logger.info(f"Tarea {task.id} marcada como fallida por compensación")
                
                # 4. Crear evento de finalización de compensación
                completion_event = ImageDeletionCompleted(
                    image_id=command.image_id,
                    task_id=command.task_id,
                    reason=command.reason
                )
                
                # Publicar evento
                await self.publisher.publish_event(completion_event)
                
                # Confirmar transacción
                await self.uow.commit()
                
                return {
                    "image_id": str(command.image_id),
                    "task_id": str(command.task_id),
                    "status": "DELETED",
                    "reason": command.reason
                }
                
            except Exception as e:
                logger.error(f"Error al eliminar imagen: {str(e)}")
                
                # Publicar evento de fallo
                failure_event = ImageDeletionFailed(
                    image_id=command.image_id,
                    task_id=command.task_id,
                    error_message=str(e),
                    reason=command.reason
                )
                
                try:
                    await self.publisher.publish_event(failure_event)
                except Exception as pub_error:
                    logger.error(f"Error al publicar evento de fallo: {str(pub_error)}")
                
                # No es necesario hacer rollback explícito ya que el context manager lo hará
                raise e


# Funciones para ayudar a ejecutar los comandos
async def delete_retrieved_image(
    handler: DeleteRetrievedImageHandler,
    image_id: uuid.UUID,
    task_id: uuid.UUID,
    reason: str = "Compensación de saga"
) -> Dict[str, Any]:
    """Ejecuta el comando DeleteRetrievedImage"""
    command = DeleteRetrievedImageCommand(
        image_id=image_id,
        task_id=task_id,
        reason=reason
    )
    return await handler.handle(command)