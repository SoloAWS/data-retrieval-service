import os
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """Configuración global de la aplicación"""

    # Configuración general
    environment: str = Field(default="dev")
    log_level: str = Field(default="INFO")

    # Configuración de la base de datos
    db_host: str = Field(default="postgres")
    db_port: int = Field(default=5432)
    db_user: str = Field(default="user")
    db_password: str = Field(default="password")
    db_name: str = Field(default="anonymization_db")

    @property
    def db_url(self) -> str:
        """URL de conexión a la base de datos"""
        return os.getenv(
            "DATABASE_URL",
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}",
        )

    # Configuración de Pulsar
    pulsar_service_url: str = Field(default="pulsar://pulsar-broker:6650")
    pulsar_token: str = Field(default="")
    
    # Nuevo: Configuración del consumidor de Pulsar
    pulsar_subscription_name: str = Field(default="data-retrieval-service")
    pulsar_consumer_topics: list = Field(default=["persistent://public/default/data-retrieval-commands"])
    pulsar_consumer_max_workers: int = Field(default=5)
    pulsar_consumer_batch_size: int = Field(default=10)
    pulsar_consumer_flow_control_size: int = Field(default=100)
    pulsar_consumer_receive_queue_size: int = Field(default=1000)
    
    # Mapeo de eventos a tópicos de Pulsar
    pulsar_event_topics_mapping: dict = Field(default={
        "RetrievalStarted": "persistent://public/default/retrieval-started",
        "RetrievalCompleted": "persistent://public/default/retrieval-completed",
        "RetrievalFailed": "persistent://public/default/retrieval-failed",
        "ImagesRetrieved": "persistent://public/default/images-retrieved",
        "ImageReadyForAnonymization": "persistent://public/default/image-anonymization",
        "ImageUploadFailed": "persistent://public/default/image-upload-failed"
    })

    # Configuración del API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_reload: bool = Field(default=True)
    
    # Configuración de almacenamiento de imágenes
    image_storage_path: str = Field(default="/tmp/data_retrieval_images")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """Retorna una instancia de la configuración cacheada"""
    return Settings()