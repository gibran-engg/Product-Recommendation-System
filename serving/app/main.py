from fastapi import FastAPI

app = FastAPI(
    title="Product Recommendation System API",
    version="0.1.0",
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


@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: str):
    return {
        "user_id": user_id,
        "recommendations": [
            {"item_id": "1001", "score": 0.95},
            {"item_id": "1002", "score": 0.89},
            {"item_id": "1003", "score": 0.84},
            {"item_id": "1004", "score": 0.78},
            {"item_id": "1005", "score": 0.72},
        ],
    }