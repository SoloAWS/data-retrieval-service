# Data Retrieval Service

Microservicio para la recuperación y carga de imágenes médicas en SaludTech de los Alpes.

## Descripción

Este microservicio forma parte del sistema SaludTech, encargado de procesar y distribuir imágenes médicas para entrenamiento de modelos de IA. El servicio "data-retrieval-service" específicamente se encarga de la adquisición y almacenamiento inicial de las imágenes médicas, preparándolas para su posterior anonimización.

## Arquitectura

El servicio está desarrollado siguiendo la arquitectura hexagonal (también conocida como Ports and Adapters) con principios de Domain-Driven Design (DDD):

- **Dominio**: Contiene la lógica de negocio pura, con entidades, objetos de valor y reglas de negocio
- **Aplicación**: Orquesta los casos de uso usando el dominio
- **Infraestructura**: Implementa los detalles técnicos (base de datos, mensajería, etc.)
- **API**: Expone la funcionalidad a través de endpoints REST versionados

### Patrón Unit of Work

El servicio implementa el patrón Unit of Work (UoW) para gestionar transacciones y garantizar la integridad de los datos:

- Proporciona límites de transacción claros
- Gestiona la coherencia entre múltiples operaciones de repositorio
- Simplifica el manejo de errores mediante rollback automático
- Permite una mejor separación de responsabilidades

### Presentación Versionada

La capa de presentación está estructurada siguiendo un enfoque de versionado API:

- Los endpoints se organizan bajo `/api/v{número_versión}/`
- La versión inicial es `/api/v1/`
- Esta estructura permite evolucionar la API sin romper clientes existentes

## Funcionalidades principales

El servicio proporciona las siguientes funcionalidades:

- Creación y gestión de tareas de recuperación de imágenes médicas
- Almacenamiento de imágenes médicas en el sistema de archivos
- Notificación al servicio de anonimización cuando una imagen está lista para ser procesada
- Seguimiento del estado de las tareas de recuperación

## Comunicación con otros servicios

El servicio se comunica con otros componentes del ecosistema a través de mensajería asíncrona:

- Envía eventos `ImageReadyForAnonymization` al servicio de anonimización para que procese las imágenes recuperadas
- Expone una API REST para que otros servicios puedan crear tareas de recuperación y consultar su estado

## Tecnologías utilizadas

- **Python 3.9+**: Lenguaje de programación principal
- **FastAPI**: Framework para el desarrollo de APIs REST
- **SQLAlchemy**: ORM para interacción con la base de datos
- **Apache Pulsar**: Sistema de mensajería para comunicación entre servicios
- **PostgreSQL**: Base de datos relacional para almacenar metadatos

## Estructura del proyecto

```
data_retrieval_service/
├── api/                          # Capa de presentación versionada
│   ├── __init__.py               # Router principal
│   └── v1/                       # API versión 1
│       ├── __init__.py
│       └── data_retrieval.py     # Endpoints del servicio
├── config/                       # Configuración global
│   ├── database.py               # Configuración de base de datos
│   ├── dependencies.py           # Proveedores de dependencias
│   └── settings.py               # Configuración general
├── modules/
│   └── data_retrieval/           # Módulo principal
│       ├── application/          # Capa de aplicación
│       │   ├── commands/         # Comandos (casos de uso de escritura)
│       │   │   ├── commands.py   # Implementación tradicional
│       │   │   └── uow_commands.py # Implementación con UoW
│       │   ├── events/           # Manejadores de eventos
│       │   └── queries/          # Consultas (casos de uso de lectura)
│       │       ├── queries.py    # Implementación tradicional
│       │       └── uow_queries.py # Implementación con UoW
│       ├── domain/               # Capa de dominio
│       │   ├── entities.py       # Entidades del dominio
│       │   ├── events.py         # Eventos del dominio
│       │   ├── repositories.py   # Interfaces de repositorios
│       │   └── value_objects.py  # Objetos de valor
│       └── infrastructure/       # Capa de infraestructura
│           ├── messaging/        # Implementación de mensajería
│           └── persistence/      # Implementación de persistencia
├── seedwork/                     # Clases base compartidas
│   ├── application/              # Patrones de aplicación
│   ├── domain/                   # Patrones de dominio
│   │   ├── repositories.py       # Repositorio base
│   │   └── ...
│   └── infrastructure/           # Patrones de infraestructura
│       ├── uow.py                # Implementación de Unit of Work
│       └── ...
└── main.py                       # Punto de entrada de la aplicación
```

## Configuración

La configuración se realiza a través de variables de entorno o un archivo `.env` con las siguientes variables:

```
# Entorno
ENVIRONMENT=dev
LOG_LEVEL=INFO

# Base de datos
DB_HOST=localhost
DB_PORT=5432
DB_USER=user
DB_PASSWORD=password
DB_NAME=data_retrieval_db

# API
API_HOST=0.0.0.0
API_PORT=8000
API_RELOAD=true

# Pulsar
PULSAR_SERVICE_URL=pulsar://localhost:6650

# Almacenamiento
IMAGE_STORAGE_PATH=/tmp/data_retrieval_images
```

## Instalación y ejecución

### Requisitos previos

- Python 3.9+
- PostgreSQL
- Apache Pulsar

### Instalación

1. Clonar el repositorio
2. Crear un entorno virtual: `python -m venv venv`
3. Activar el entorno virtual:
   - Windows: `venv\Scripts\activate`
   - Unix/MacOS: `source venv/bin/activate`
4. Instalar dependencias: `pip install -r requirements.txt`
5. Configurar variables de entorno o crear archivo `.env`

### Ejecución

```bash
# Ejecutar la aplicación
python -m data_retrieval_service.main

# Alternativamente con uvicorn directamente
uvicorn data_retrieval_service.main:app --reload
```

## API REST

La API REST del servicio ofrece los siguientes endpoints principales:

- `POST /api/v1/data-retrieval/tasks`: Crea una nueva tarea de recuperación
- `GET /api/v1/data-retrieval/tasks/{task_id}`: Obtiene información de una tarea específica
- `POST /api/v1/data-retrieval/tasks/{task_id}/start`: Inicia una tarea de recuperación
- `POST /api/v1/data-retrieval/tasks/{task_id}/images`: Carga una imagen para una tarea
- `GET /api/v1/data-retrieval/tasks/{task_id}/images`: Obtiene las imágenes de una tarea
- `GET /api/v1/data-retrieval/tasks`: Obtiene tareas con filtros (fuente, lote, pendientes)

La documentación completa de la API está disponible en:
- Swagger UI: `/docs`
- ReDoc: `/redoc`

## Flujo de trabajo

1. Se crea una tarea de recuperación (`POST /api/v1/data-retrieval/tasks`)
2. Se inicia la tarea (`POST /api/v1/data-retrieval/tasks/{id}/start`)
3. Se cargan las imágenes para la tarea (`POST /api/v1/data-retrieval/tasks/{id}/images`)
4. Por cada imagen cargada, se genera un evento `ImageReadyForAnonymization`
5. El servicio de anonimización recibe estos eventos y procesa las imágenes
6. Al finalizar, se marca la tarea como completada (`POST /api/v1/data-retrieval/tasks/{id}/complete`)

## Implementación del Patrón Unit of Work

### Beneficios de Unit of Work

- **Integridad Transaccional**: Todas las operaciones dentro de un caso de uso se ejecutan en una única transacción
- **Gestión de Dependencias Más Limpia**: Los servicios dependen del UoW, no de repositorios individuales
- **Manejo de Errores Simplificado**: Rollback automático en caso de excepciones
- **Gestión Consistente de Sesiones**: Una sesión por request

### Uso del Patrón

```python
async def handle_command(command, uow):
    async with uow:
        # Obtener repositorios
        repository_a = uow.repository('repo_a')
        repository_b = uow.repository('repo_b')
        
        # Realizar operaciones
        entity = await repository_a.get_by_id(command.id)
        entity.do_something()
        await repository_a.update(entity)
        
        # Los cambios se confirman (commit) automáticamente al salir
        # del contexto si no hay excepciones, o se hace rollback si las hay
```

## Integración con servicio de anonimización

En su lugar, cuando se carga una imagen, se emite un evento `ImageReadyForAnonymization` con la siguiente información:

```json
{
  "image_id": "uuid-de-la-imagen",
  "task_id": "uuid-de-la-tarea",
  "source": "nombre-de-la-fuente",
  "modality": "tipo-de-modalidad",
  "region": "region-anatomica",
  "file_path": "ruta-al-archivo-en-disco"
}
```

El servicio de anonimización debe escuchar estos eventos en el tópico `anonymization-requests` y procesar las imágenes indicadas.