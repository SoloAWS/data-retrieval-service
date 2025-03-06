import logging
from typing import Dict, Type, Optional
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ..seedwork.infrastructure.uow import SqlAlchemyUnitOfWork
from ..seedwork.domain.repositories import Repository
from ..config.database import get_db, create_session
from ..modules.data_retrieval.domain.repositories import RetrievalRepository, ImageRepository
from ..modules.data_retrieval.infrastructure.persistence.repositories import SQLRetrievalRepository, SQLImageRepository
from ..modules.data_retrieval.infrastructure.messaging.pulsar_publisher import PulsarPublisher

logger = logging.getLogger(__name__)

# Instancias singleton
_publisher_instance = None
_consumer_instance = None

def setup_messaging(publisher: PulsarPublisher):
    """Set up the messaging dependencies with the initialized publisher"""
    global _publisher_instance
    _publisher_instance = publisher
    logger.info("Messaging dependencies initialized with publisher")

def get_publisher():
    """Returns the PulsarPublisher singleton"""
    return _publisher_instance

# Repository factories
def get_retrieval_repository(db: AsyncSession = Depends(get_db)):
    """Returns a RetrievalRepository implementation"""
    return SQLRetrievalRepository(db)

def get_image_repository(db: AsyncSession = Depends(get_db)):
    """Returns an ImageRepository implementation"""
    return SQLImageRepository(db)

# UnitOfWork related dependencies
def get_repository_factories() -> Dict[str, Type[Repository]]:
    """Returns a dictionary of repository factories for UoW"""
    return {
        'retrieval': SQLRetrievalRepository,
        'image': SQLImageRepository
    }

def get_unit_of_work():
    """Returns a new SqlAlchemyUnitOfWork instance"""
    repositories_factories = get_repository_factories()
    return SqlAlchemyUnitOfWork(create_session, repositories_factories)

def create_consumer(settings):
    """Creates a new PulsarConsumer instance"""
    global _publisher_instance
    
    # Importación tardía para evitar ciclos
    from ..modules.data_retrieval.infrastructure.messaging.pulsar_consumer import PulsarConsumer
    from ..modules.data_retrieval.application.commands.command_handlers import command_handlers
    
    consumer_config = {
        "receiver_queue_size": settings.pulsar_consumer_receive_queue_size,
        "max_total_receiver_queue_size_across_partitions": settings.pulsar_consumer_flow_control_size,
        "consumer_name": f"data-retrieval-{settings.environment}"
    }
    
    consumer = PulsarConsumer(
        service_url=settings.pulsar_service_url,
        subscription_name=settings.pulsar_subscription_name,
        topics=settings.pulsar_consumer_topics,
        command_handlers=command_handlers,
        token=settings.pulsar_token,
        consumer_config=consumer_config,
        publisher=_publisher_instance,
        max_workers=settings.pulsar_consumer_max_workers,
        get_unit_of_work_func=get_unit_of_work
    )
    
    return consumer

def set_consumer(consumer):
    """Sets the global consumer instance"""
    global _consumer_instance
    _consumer_instance = consumer

def get_consumer():
    """Returns the PulsarConsumer singleton"""
    return _consumer_instance