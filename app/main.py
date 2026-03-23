from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from app.db import engine

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Python Data Service Running 🚀"}

@app.get("/tables")
def get_tables():
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            return {"tables": [row[0] for row in result]}
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=500, detail=f"Database query failed: {exc}") from exc