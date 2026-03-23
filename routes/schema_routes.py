from fastapi import APIRouter, HTTPException
from config import get_schema

router = APIRouter()

@router.get("/tables")
async def list_tables():
    graph = get_schema()
    return {
        name: {
            "is_junction":  t.is_junction,
            "natural_key":  t.natural_key,
            "column_count": len(t.columns),
        }
        for name, t in graph.tables.items()
    }

@router.get("/tables/{table_name}")
async def inspect_table(table_name: str):
    graph = get_schema()
    if table_name not in graph.tables:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")
    table = graph.tables[table_name]
    return {
        "name":        table.name,
        "is_junction": table.is_junction,
        "natural_key": table.natural_key,
        "columns": [
            {
                "name":        c.name,
                "type":        c.data_type,
                "required":    c.is_required,
                "unique":      c.is_unique,
                "pk":          c.is_primary_key,
                "enum_values": c.enum_values,
                "fk_table":    c.fk_table,
                "fk_column":   c.fk_column,
            }
            for c in table.columns
        ],
    }

@router.get("/fk-graph")
async def fk_graph():
    graph = get_schema()
    return [
        {"from": f"{e.from_table}.{e.from_col}", "to": f"{e.to_table}.{e.to_col}"}
        for e in graph.fk_edges
    ]

@router.get("/junctions")
async def junction_tables():
    graph = get_schema()
    return [name for name, t in graph.tables.items() if t.is_junction]