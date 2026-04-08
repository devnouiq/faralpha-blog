"""Shared DuckDB upsert helper for pipeline stages."""

from __future__ import annotations

from faralpha.utils.logger import get_logger

log = get_logger("upsert")


def upsert_by_market(con, table: str, df, markets: list[str]) -> None:
    """Market-scoped upsert: delete rows for processed markets, then insert.

    On schema change, recreates table and warns about lost data.
    """
    exists = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_schema='main' AND table_name='{table}'"
    ).fetchone()[0]
    if not exists:
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
        return

    old_col_names = {
        r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()
    }
    new_col_names = set(df.columns)

    if old_col_names == new_col_names:
        for mkt in markets:
            con.execute(f"DELETE FROM {table} WHERE market = ?", [mkt])
        con.execute(f"INSERT INTO {table} SELECT * FROM df")
    else:
        mkt_placeholders = ", ".join(["?"] * len(markets))
        other_count = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE market NOT IN ({mkt_placeholders})",
            markets,
        ).fetchone()[0]
        con.execute(f"DROP TABLE {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
        if other_count > 0:
            log.warning(
                f"Schema change on '{table}': {other_count:,} rows from "
                f"other market(s) dropped — re-run pipeline for those markets"
            )
