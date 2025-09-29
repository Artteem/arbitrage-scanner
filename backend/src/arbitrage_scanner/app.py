from fastapi import FastAPI
from .settings import settings

app = FastAPI(title="Arbitrage Scanner API", version="0.1.0")

@app.get("/health")
async def health():
    return {"status": "ok", "env": settings.model_dump()}
