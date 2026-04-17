"""Shared upsert helper for pipeline stages (DuckDB or PostgreSQL)."""

from __future__ import annotations

import pandas as pd

from faralpha.config import use_postgres_database
from faralpha.utils.logger import get_logger

log = get_logger("upsert")


def _duck_register_df(con, df: pd.DataFrame) -> None:
    """Register pandas ``df`` so ``FROM df`` works in DuckDB SQL."""
    try:
        con.connection.register("df", df)
    except Exception as e:
        log.debug("DuckDB register('df'): %s", e)


def _pg_raw(con):
    return getattr(con, "_raw", con)


def _pg_sql_type(dtype) -> str:
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    return "TEXT"


def _pg_create_table_from_df(pg_con, table: str, df: pd.DataFrame) -> None:
    parts = []
    for col in df.columns:
        parts.append(f'"{col}" {_pg_sql_type(df[col].dtype)}')
    ddl = f'CREATE TABLE "{table}" ({", ".join(parts)})'
    pg_con.execute(ddl)


def _pg_cell(val):
    """pandas / numpy scalars → None or native Python types for psycopg2."""
    import math

    import numpy as np

    if val is None:
        return None

    if isinstance(val, np.datetime64):
        if pd.isna(val):
            return None
        return pd.Timestamp(val).to_pydatetime()
    if isinstance(val, np.timedelta64):
        if pd.isna(val):
            return None
        return pd.Timedelta(val).to_pytimedelta()

    if isinstance(val, np.generic):
        val = val.item()

    if isinstance(val, float) and math.isnan(val):
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def _pg_tuples(df: pd.DataFrame) -> list[tuple]:
    return [tuple(_pg_cell(x) for x in row) for row in df.itertuples(index=False, name=None)]


def _pg_insert_df(pg_con, table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    from psycopg2.extras import execute_values

    raw = _pg_raw(pg_con)
    cols = list(df.columns)
    col_list = ", ".join(f'"{c}"' for c in cols)
    tuples = _pg_tuples(df)
    sql = f'INSERT INTO "{table}" ({col_list}) VALUES %s'
    execute_values(raw, sql, tuples, page_size=8000)


def _table_exists_pg(con, table: str) -> bool:
    r = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'public' AND LOWER(table_name) = LOWER(?)",
        [table],
    ).fetchone()[0]
    return r > 0


def _column_names_pg(con, table: str) -> set[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND LOWER(table_name) = LOWER(?) "
        "ORDER BY ordinal_position",
        [table],
    ).fetchall()
    return {r[0] for r in rows}


def upsert_by_market(con, table: str, df: pd.DataFrame, markets: list[str]) -> None:
    """Market-scoped upsert: delete rows for processed markets, then insert.

    DuckDB: legacy SQL (``FROM df``, ``PRAGMA``) when Postgres is off.
    PostgreSQL: portable DDL + batched INSERT (no ``PRAGMA``, ``public`` schema).
    """
    if use_postgres_database():
        _upsert_by_market_postgres(con, table, df, markets)
    else:
        _upsert_by_market_duckdb(con, table, df, markets)


def _upsert_by_market_duckdb(con, table: str, df: pd.DataFrame, markets: list[str]) -> None:
    exists = (
        con.execute(
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema='main' AND table_name='{table}'"
        ).fetchone()[0]
        > 0
    )
    if not exists:
        _duck_register_df(con, df)
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
        return

    old_col_names = {r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
    new_col_names = set(df.columns)

    if old_col_names == new_col_names:
        for mkt in markets:
            con.execute(f"DELETE FROM {table} WHERE market = ?", [mkt])
        _duck_register_df(con, df)
        con.execute(f"INSERT INTO {table} SELECT * FROM df")
    else:
        mkt_placeholders = ", ".join(["?"] * len(markets))
        other_count = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE market NOT IN ({mkt_placeholders})",
            markets,
        ).fetchone()[0]
        con.execute(f"DROP TABLE {table}")
        _duck_register_df(con, df)
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
        if other_count > 0:
            log.warning(
                f"Schema change on '{table}': {other_count:,} rows from "
                f"other market(s) dropped — re-run pipeline for those markets"
            )


def _upsert_by_market_postgres(con, table: str, df: pd.DataFrame, markets: list[str]) -> None:
    exists = _table_exists_pg(con, table)
    if not exists:
        _pg_create_table_from_df(con, table, df)
        if not df.empty:
            _pg_insert_df(con, table, df)
        return

    old_col_names = _column_names_pg(con, table)
    new_col_names = set(df.columns)
    if {c.lower() for c in old_col_names} != {c.lower() for c in new_col_names}:
        mkt_placeholders = ", ".join(["?"] * len(markets))
        other_count = con.execute(
            f"SELECT COUNT(*) FROM \"{table}\" WHERE market NOT IN ({mkt_placeholders})",
            markets,
        ).fetchone()[0]
        con.execute(f'DROP TABLE "{table}" CASCADE')
        _pg_create_table_from_df(con, table, df)
        if not df.empty:
            _pg_insert_df(con, table, df)
        if other_count > 0:
            log.warning(
                f"Schema change on '{table}': {other_count:,} rows from "
                f"other market(s) dropped — re-run pipeline for those markets"
            )
        return

    for mkt in markets:
        con.execute(f'DELETE FROM "{table}" WHERE market = ?', [mkt])
    if not df.empty:
        _pg_insert_df(con, table, df)
