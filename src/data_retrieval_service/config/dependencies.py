import logging
from typing import Dict, Type
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ..seedwork.infrastructure.uow import SqlAlchemyUnitOfWork
from ..seedwork.domain.repositories import Repository
from ..config.database import get_db, create_session
from ..modules.data_retrieval.domain.repositories import RetrievalRepository, ImageRepository
from ..modules.data_retrieval.infrastructure.persistence.repositories import SQLRetrievalRepository, SQLImageRepository
from ..modules.data_retrieval.infrastructure.messaging.pulsar_publisher import PulsarPublisher

logger = logging.getLogger(__name__)

_publisher_instance = None


def setup_dependencies(publisher: PulsarPublisher):
    """Set up the dependencies with the initialized publisher"""
    global _publisher_instance
    _publisher_instance = publisher
    logger.info("Dependencies initialized with publisher")


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