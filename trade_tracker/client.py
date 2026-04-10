from __future__ import annotations

from typing import Any
import xml.etree.ElementTree as ET

import requests

from trade_tracker.config import Settings


class CustomsTradeClient:
    def __init__(self, settings: Settings, timeout: int = 30) -> None:
        self.settings = settings
        self.timeout = timeout

    def fetch_country_trade(
        self,
        start_yymm: str,
        end_yymm: str,
        country_code: str | None = None,
    ) -> list[dict[str, str]]:
        params = {
            "serviceKey": self.settings.service_key,
            "strtYymm": start_yymm,
            "endYymm": end_yymm,
        }
        if country_code:
            params["cntyCd"] = country_code.upper()
        return self._request(self.settings.country_service_url, params)

    def fetch_item_trade(
        self,
        start_yymm: str,
        end_yymm: str,
        hs_code: str | None = None,
    ) -> list[dict[str, str]]:
        params = {
            "serviceKey": self.settings.service_key,
            "strtYymm": start_yymm,
            "endYymm": end_yymm,
        }
        if hs_code:
            params["hsSgn"] = hs_code
        return self._request(self.settings.item_service_url, params)

    def _request(self, url: str, params: dict[str, Any]) -> list[dict[str, str]]:
        if not self.settings.service_key:
            raise ValueError("PUBLIC_DATA_API_KEY is not set.")

        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        self._raise_for_api_error(root)
        return self._extract_items(root)

    def _raise_for_api_error(self, root: ET.Element) -> None:
        result_code = root.findtext(".//resultCode")
        result_msg = root.findtext(".//resultMsg")
        if result_code and result_code != "00":
            raise RuntimeError(f"API error {result_code}: {result_msg}")

    def _extract_items(self, root: ET.Element) -> list[dict[str, str]]:
        items = root.findall(".//item")
        parsed_items: list[dict[str, str]] = []

        for item in items:
            record = {}
            for child in item:
                record[child.tag] = (child.text or "").strip()
            if record:
                parsed_items.append(record)

        return parsed_items

