import logging
import asyncio
import time
from typing import Dict
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config.settings import get_settings
from .config.database import init_db
from .config.dependencies import (
    setup_messaging, 
    create_consumer, 
    set_consumer,
    get_consumer,
    get_publisher
)
from .modules.data_retrieval.infrastructure.messaging.pulsar_publisher import PulsarPublisher
from .api import api_router

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Obtener configuración
settings = get_settings()

# Crear la aplicación FastAPI
app = FastAPI(
    title="Data Retrieval Service",
    description="Servicio para recuperación de imágenes médicas",
    version="1.0.0"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configurar rutas de API
app.include_router(api_router, prefix="/api")

# Ruta por defecto
@app.get("/")
async def root():
    return {"message": "Data Retrieval Service API"}

# Endpoint de health check
@app.get("/data-retrieval/health", tags=["health"])
async def health_check():
    """Endpoint para verificar el estado del servicio"""
    consumer = get_consumer()
    consumer_status = "running" if consumer and consumer._is_running else "stopped"
    
    return {
        "status": "ok",
        "service": "data-retrieval-service",
        "version": "1.0.0",
        "consumer_status": consumer_status
    }

# Inicialización de la aplicación
@app.on_event("startup")
async def startup_event():
    logger.info("Iniciando servicio de recuperación de datos")
    
    # Inicializar la base de datos
    try:
        await init_db()
        logger.info("Base de datos inicializada correctamente")
    except Exception as e:
        logger.error(f"Error al inicializar la base de datos: {str(e)}")
        raise
    
    # Asegurarse de que exista el directorio de almacenamiento
    os.makedirs(settings.image_storage_path, exist_ok=True)
    
    # Inicializar el publicador de Pulsar
    try:
        publisher = PulsarPublisher(
            service_url=settings.pulsar_service_url,
            topics_mapping=settings.pulsar_event_topics_mapping,
            token=settings.pulsar_token,
        )
        
        # Configurar dependencias
        setup_messaging(publisher)
        logger.info("Publicador de Pulsar inicializado correctamente")
        
        # Inicializar el consumidor de Pulsar
        if settings.pulsar_service_url and settings.pulsar_consumer_topics:
            consumer = create_consumer(settings)
            set_consumer(consumer)
            
            # Iniciar el consumidor asíncronamente
            await consumer.start()
            logger.info("Consumidor de Pulsar iniciado correctamente")
        else:
            logger.warning("Pulsar consumer configuration missing, command processing disabled")
    except Exception as e:
        logger.error(f"Error al inicializar la mensajería: {str(e)}")
        logger.warning("Continuando sin mensajería configurada")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Cerrando servicio de recuperación de datos")
    
    # Detener el consumidor de Pulsar
    consumer = get_consumer()
    if consumer:
        try:
            await consumer.stop()
            logger.info("Consumidor de Pulsar detenido correctamente")
        except Exception as e:
            logger.error(f"Error al detener el consumidor de Pulsar: {str(e)}")


# Configuración para ejecutar directamente
if __name__ == "__main__":
    import uvicorn
    
    settings = get_settings()
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
        log_level=settings.log_level.lower()
    )