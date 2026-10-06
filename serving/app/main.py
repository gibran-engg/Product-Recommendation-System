from fastapi import FastAPI

from app.routes.recommendations import router as recommendations_router


app = FastAPI(
    title="Product Recommendation System API",
    version="0.2.0",
)


@app.get("/")
def root():
    return {
        "service": "recommendation-api",
        "status": "running",
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
    }


app.include_router(recommendations_router)