from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    service_key: str
    dart_api_key: str
    db_path: str = "data/trade_tracker.db"
    country_service_url: str = "http://apis.data.go.kr/1220000/nationtrade/getNationtradeList"
    item_service_url: str = "http://apis.data.go.kr/1220000/Itemtrade/getItemtradeList"


def get_settings() -> Settings:
    service_key = os.getenv("PUBLIC_DATA_API_KEY", "").strip()
    dart_api_key = os.getenv("DART_API_KEY", "").strip()
    db_path = os.getenv("TRADE_DB_PATH", "data/trade_tracker.db").strip()
    return Settings(service_key=service_key, dart_api_key=dart_api_key, db_path=db_path)
