from fastapi import FastAPI
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
    result = engine.execute(query)
    return {"tables": [row[0] for row in result]}