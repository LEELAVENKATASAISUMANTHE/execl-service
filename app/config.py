import asyncpg
from app.schema import ColumnMeta, FKEdge, TableMeta, SchemaGraph

# ── Filters ───────────────────────────────────────────────────────────
EXCLUDED_PREFIXES = ("nc_", "directus_", "xc_", "_prisma", "pg_")
EXCLUDED_TABLES   = {
    "databasechangelog", "notification",
    "workspace", "workspace_user"
}
SYSTEM_COLS = {"created_at", "updated_at", "deleted_at"}

PREFERRED_LOOKUP_NAMES = [
    "email", "roll_number", "registration_number",
    "employee_id", "code", "slug", "username",
    "phone", "name", "title",
]

_schema_graph: SchemaGraph | None = None


# ══════════════════════════════════════════════════════════════════════
#  QUERIES
# ══════════════════════════════════════════════════════════════════════

async def _get_all_tables(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type   = 'BASE TABLE'
        ORDER BY table_name
    """)
    return [
        r["table_name"] for r in rows
        if not any(r["table_name"].startswith(p) for p in EXCLUDED_PREFIXES)
        and r["table_name"] not in EXCLUDED_TABLES
    ]


async def _get_enums(conn: asyncpg.Connection) -> dict[str, list[str]]:
    rows = await conn.fetch("""
        SELECT
            t.typname AS enum_name,
            array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        GROUP BY t.typname
    """)
    return {r["enum_name"]: list(r["values"]) for r in rows}


async def _get_fk_edges(conn: asyncpg.Connection) -> list[FKEdge]:
    rows = await conn.fetch("""
        SELECT
            kcu.table_name  AS from_table,
            kcu.column_name AS from_col,
            ccu.table_name  AS to_table,
            ccu.column_name AS to_col
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
           AND tc.table_schema    = kcu.table_schema
        JOIN information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
        JOIN information_schema.key_column_usage ccu
            ON rc.unique_constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema    = 'public'
    """)
    return [
        FKEdge(r["from_table"], r["from_col"], r["to_table"], r["to_col"])
        for r in rows
    ]


async def _get_pk_cols(conn: asyncpg.Connection, table: str) -> set[str]:
    rows = await conn.fetch("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name      = $1
          AND tc.table_schema    = 'public'
          AND tc.constraint_type = 'PRIMARY KEY'
    """, table)
    return {r["column_name"] for r in rows}


async def _get_unique_cols(conn: asyncpg.Connection, table: str) -> set[str]:
    rows = await conn.fetch("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name      = $1
          AND tc.table_schema    = 'public'
          AND tc.constraint_type = 'UNIQUE'
    """, table)
    return {r["column_name"] for r in rows}


async def _get_columns(
    conn: asyncpg.Connection,
    table: str,
    enums: dict,
    pk_cols: set,
    unique_cols: set,
    fk_map: dict,
) -> list[ColumnMeta]:
    rows = await conn.fetch("""
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            udt_name
        FROM information_schema.columns
        WHERE table_name   = $1
          AND table_schema = 'public'
        ORDER BY ordinal_position
    """, table)

    columns = []
    for r in rows:
        col_name = r["column_name"]
        is_pk    = col_name in pk_cols
        fk_info  = fk_map.get(table, {}).get(col_name)

        columns.append(ColumnMeta(
            name           = col_name,
            data_type      = r["data_type"],
            is_required    = (
                r["is_nullable"]     == "NO"
                and r["column_default"] is None
                and not is_pk
            ),
            is_nullable    = r["is_nullable"] == "YES",
            is_primary_key = is_pk,
            is_unique      = col_name in unique_cols,
            default        = r["column_default"],
            max_length     = r["character_maximum_length"],
            enum_values    = enums.get(r["udt_name"]) if r["data_type"] == "USER-DEFINED" else None,
            fk_table       = fk_info.to_table  if fk_info else None,
            fk_column      = fk_info.to_col    if fk_info else None,
            udt_name       = r["udt_name"],
        ))
    return columns


# ══════════════════════════════════════════════════════════════════════
#  INFERENCE PASSES
# ══════════════════════════════════════════════════════════════════════

def _classify_junctions(graph: SchemaGraph) -> None:
    for table in graph.tables.values():
        meaningful = [
            c for c in table.columns
            if not c.is_primary_key and c.name not in SYSTEM_COLS
        ]
        fk_cols    = [c for c in meaningful if c.fk_table is not None]
        fk_targets = {c.fk_table for c in fk_cols}

        if (
            len(fk_targets) >= 2
            and len(meaningful) > 0
            and len(fk_cols) / len(meaningful) >= 0.5
        ):
            table.is_junction = True


def _infer_natural_key(table: TableMeta) -> str | None:
    col_map = {c.name: c for c in table.columns}

    # Pass 1 — preferred name that is unique or required
    for name in PREFERRED_LOOKUP_NAMES:
        if name in col_map:
            col = col_map[name]
            if col.is_unique or col.is_required:
                return name

    # Pass 2 — any unique non-PK non-system column
    for col in table.columns:
        if col.is_unique and not col.is_primary_key and col.name not in SYSTEM_COLS:
            return col.name

    return None


# ══════════════════════════════════════════════════════════════════════
#  MASTER BUILDER
# ══════════════════════════════════════════════════════════════════════

async def build_schema_graph(pool: asyncpg.Pool) -> SchemaGraph:
    async with pool.acquire() as conn:

        # Step 1 — DB-wide queries (run once)
        table_names = await _get_all_tables(conn)
        enums       = await _get_enums(conn)
        fk_edges    = await _get_fk_edges(conn)

        # Step 2 — Build FK lookup map
        fk_map: dict[str, dict[str, FKEdge]] = {}
        for edge in fk_edges:
            fk_map.setdefault(edge.from_table, {})[edge.from_col] = edge

        # Step 3 — Build each table
        tables: dict[str, TableMeta] = {}
        for table_name in table_names:
            pk_cols     = await _get_pk_cols(conn, table_name)
            unique_cols = await _get_unique_cols(conn, table_name)
            columns     = await _get_columns(
                conn, table_name, enums, pk_cols, unique_cols, fk_map
            )
            tables[table_name] = TableMeta(name=table_name, columns=columns)

    # Step 4 — Inference (pure Python, no DB needed)
    graph = SchemaGraph(tables=tables, fk_edges=fk_edges, fk_map=fk_map)
    _classify_junctions(graph)
    for table in graph.tables.values():
        table.natural_key = _infer_natural_key(table)

    return graph


async def init_schema(pool: asyncpg.Pool) -> SchemaGraph:
    global _schema_graph
    _schema_graph = await build_schema_graph(pool)
    print(f"✅ Schema graph built — {len(_schema_graph.tables)} tables discovered.")
    return _schema_graph


def get_schema() -> SchemaGraph:
    if _schema_graph is None:
        raise RuntimeError("Schema not initialized. Call init_schema first.")
    return _schema_graph