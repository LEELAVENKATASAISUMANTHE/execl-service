import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from db import init_pool, close_pool, get_pool
from config import init_schema
from routes.schema_routes import router as schema_router

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await init_pool()
    await init_schema(pool)
    yield
    await close_pool()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def home():
    return {"message": "Python Data Service Running 🚀"}

@app.get("/tables")
async def get_tables():
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            return {"tables": [r["table_name"] for r in rows]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

app.include_router(schema_router, prefix="/schema", tags=["Schema"])