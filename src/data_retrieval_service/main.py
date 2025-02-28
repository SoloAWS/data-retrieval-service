import logging
import asyncio
from typing import Dict
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config.settings import get_settings
from .config.database import init_db
from .config.dependencies import setup_dependencies
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


# Mapeo de eventos a temas de Pulsar
PULSAR_TOPICS_MAPPING: Dict[str, str] = {
    "RetrievalStarted": "persistent://public/default/retrieval-started",
    "RetrievalCompleted": "persistent://public/default/retrieval-completed",
    "RetrievalFailed": "persistent://public/default/retrieval-failed",
    "ImagesRetrieved": "persistent://public/default/images-retrieved",
    "ImageReadyForAnonymization": "persistent://public/default/image-anonymization"
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
            topics_mapping=PULSAR_TOPICS_MAPPING
        )
        
        # Configurar dependencias
        setup_dependencies(publisher)
        logger.info("Publicador de Pulsar inicializado correctamente")
    except Exception as e:
        logger.error(f"Error al inicializar el publicador de Pulsar: {str(e)}")
        logger.warning("Continuando sin publicador de Pulsar configurado")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Cerrando servicio de recuperación de datos")
    # Aquí se podrían agregar tareas de limpieza si son necesarias
    
    
# Ruta por defecto
@app.get("/")
async def root():
    return {"message": "Data Retrieval Service API"}

# Endpoint de health check
@app.get("/data-retrieval/health", tags=["health"])
async def health_check():
    """Endpoint para verificar el estado del servicio"""
    return {
        "status": "ok",
        "service": "data-retrieval-service",
        "version": "1.0.0"
    }

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