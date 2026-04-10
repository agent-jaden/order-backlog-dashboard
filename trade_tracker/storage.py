from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pandas as pd


SCHEMA = """
CREATE TABLE IF NOT EXISTS trade_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset TEXT NOT NULL,
    period TEXT NOT NULL,
    country_code TEXT,
    country_name TEXT,
    hs_code TEXT,
    item_name TEXT,
    export_usd REAL,
    import_usd REAL,
    trade_balance_usd REAL,
    weight_kg REAL,
    raw_payload TEXT
);
"""


def initialize_database(db_path: str) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(path) as connection:
        connection.execute(SCHEMA)
        connection.commit()


def save_dataframe(df: pd.DataFrame, raw_records: list[dict], db_path: str) -> int:
    if df.empty:
        return 0

    initialize_database(db_path)
    insert_rows = []

    for normalized_row, raw_row in zip(df.to_dict(orient="records"), raw_records, strict=False):
        insert_rows.append(
            (
                normalized_row.get("dataset"),
                normalized_row.get("period").strftime("%Y-%m"),
                normalized_row.get("country_code"),
                normalized_row.get("country_name"),
                normalized_row.get("hs_code"),
                normalized_row.get("item_name"),
                normalized_row.get("export_usd"),
                normalized_row.get("import_usd"),
                normalized_row.get("trade_balance_usd"),
                normalized_row.get("weight_kg"),
                json.dumps(raw_row, ensure_ascii=False),
            )
        )

    with sqlite3.connect(db_path) as connection:
        connection.executemany(
            """
            INSERT INTO trade_records (
                dataset,
                period,
                country_code,
                country_name,
                hs_code,
                item_name,
                export_usd,
                import_usd,
                trade_balance_usd,
                weight_kg,
                raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )
        connection.commit()

    return len(insert_rows)

