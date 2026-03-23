from dataclasses import dataclass
from typing import Optional

@dataclass
class ColumnMeta:
    name: str
    data_type: str
    is_required: bool
    is_nullable: bool
    is_primary_key: bool
    is_unique: bool
    default: Optional[str]
    max_length: Optional[int]
    enum_values: Optional[list[str]]
    fk_table: Optional[str]
    fk_column: Optional[str]
    udt_name: Optional[str]

@dataclass
class TableMeta:
    name: str
    columns: list[ColumnMeta]
    is_junction: bool = False
    natural_key: Optional[str] = None

@dataclass
class FKEdge:
    from_table: str
    from_col: str
    to_table: str
    to_col: str

@dataclass
class SchemaGraph:
    tables: dict[str, TableMeta]
    fk_edges: list[FKEdge]
    fk_map: dict[str, dict[str, FKEdge]]