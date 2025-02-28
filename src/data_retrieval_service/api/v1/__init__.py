# Version 1 API initialization
from fastapi import APIRouter

# Import routers
from .data_retrieval import router as data_retrieval_router

# Create v1 router
router = APIRouter()

# Include service-specific routers
router.include_router(data_retrieval_router, prefix="/data-retrieval", tags=["Data Retrieval"])