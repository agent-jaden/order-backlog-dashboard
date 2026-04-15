from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from io import BytesIO, StringIO
from pathlib import Path
import re
import time
from zipfile import ZipFile

import pandas as pd
import requests
import xml.etree.ElementTree as ET


CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
DISCLOSURE_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOCUMENT_URL = "https://opendart.fss.or.kr/api/document.xml"
DISCLOSURE_MAIN_URL = "https://dart.fss.or.kr/dsaf001/main.do"
VIEWER_URL = "https://dart.fss.or.kr/report/viewer.do"
HTML_TARGET_SECTION_PATTERN = re.compile(r"4\.\s*(?:\([^)]*\)\s*)?매출\s*및\s*수주상황")

REGULAR_REPORT_CODES = {
    "11013": "1Q",
    "11012": "Half",
    "11014": "3Q",
    "11011": "Annual",
}

ORDER_BACKLOG_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"수주\s*잔고",
        r"수주총액",
        r"수주현황",
        r"order\s*backlog",
        r"backlog",
    ]
]

BACKLOG_HEADER_KEYWORDS = [
    "수주잔고",
    "수주 잔고",
    "기말수주잔고",
    "수주잔액",
    "계약잔액",
    "잔여기성",
]

UNIT_MULTIPLIERS = {
    "원": 1,
    "천원": 1_000,
    "백만원": 1_000_000,
    "천만원": 10_000_000,
    "억원": 100_000_000,
    "십억원": 1_000_000_000,
}

SOURCE_PRIORITY = {
    "section_table": 0,
    "xml_table": 1,
    "table": 2,
    "snippet": 3,
    "generic": 4,
}

NUMBER_PATTERN = re.compile(r"(?<!\d)(-?\d[\d,]*(?:\.\d+)?)(?!\d)")
TAG_PATTERN = re.compile(r"<[^>]+>")
WHITESPACE_PATTERN = re.compile(r"[ \t]+")


@dataclass(frozen=True)
class DartCompany:
    corp_code: str
    corp_name: str
    stock_code: str
    modify_date: str


@dataclass(frozen=True)
class DartFiling:
    corp_code: str
    corp_name: str
    stock_code: str
    report_code: str
    report_label: str
    report_name: str
    receipt_no: str
    filing_date: str


@dataclass(frozen=True)
class DartViewerNode:
    title: str
    level: int
    receipt_no: str
    document_no: str
    element_id: str
    offset: str
    length: str
    dtd: str
    toc_no: str = ""


@dataclass(frozen=True)
class OrderBacklogMatch:
    receipt_no: str
    filing_date: str
    report_name: str
    report_label: str
    source_file: str
    matched_text: str
    raw_value: str | None
    unit: str | None
    amount_krw: int | None
    source_kind: str = "generic"


class DartClient:
    def __init__(
        self,
        api_key: str,
        timeout: int = 30,
        document_source: str = "api",
        html_request_interval: float = 3.0,
        cache_dir: str | None = None,
    ) -> None:
        self.api_key = api_key.strip()
        self.timeout = timeout
        self.document_source = document_source.strip().lower()
        if self.document_source not in {"html", "api"}:
            raise ValueError("document_source must be 'html' or 'api'.")
        self.html_request_interval = max(float(html_request_interval), 0.0)
        self.cache_dir = Path(cache_dir) if cache_dir else Path("outputs") / ".dart_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._last_html_request_at = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )

    def find_company(self, query: str) -> DartCompany:
        companies = self.load_corp_codes()
        query_normalized = query.strip().lower()

        exact_matches = [
            company
            for company in companies
            if company.corp_name.lower() == query_normalized or company.stock_code == query
        ]
        if exact_matches:
            return exact_matches[0]

        partial_matches = [
            company
            for company in companies
            if query_normalized in company.corp_name.lower() or company.stock_code == query
        ]
        if not partial_matches:
            raise ValueError(f"Could not find a company matching '{query}'.")

        return partial_matches[0]

    def load_corp_codes(self) -> list[DartCompany]:
        if not self.api_key:
            raise ValueError("DART_API_KEY is not set.")

        response = self._get(
            CORP_CODE_URL,
            params={"crtfc_key": self.api_key},
        )
        response.raise_for_status()

        with ZipFile(BytesIO(response.content)) as archive:
            xml_name = next((name for name in archive.namelist() if name.lower().endswith(".xml")), None)
            if not xml_name:
                raise RuntimeError("DART corpCode response did not contain an XML file.")
            xml_bytes = archive.read(xml_name)

        root = ET.fromstring(xml_bytes)
        companies: list[DartCompany] = []
        for element in root.findall("list"):
            corp_code = (element.findtext("corp_code") or "").strip()
            corp_name = (element.findtext("corp_name") or "").strip()
            stock_code = (element.findtext("stock_code") or "").strip()
            modify_date = (element.findtext("modify_date") or "").strip()
            if corp_code and corp_name:
                companies.append(
                    DartCompany(
                        corp_code=corp_code,
                        corp_name=corp_name,
                        stock_code=stock_code,
                        modify_date=modify_date,
                    )
                )
        return companies

    def list_regular_filings(
        self,
        corp_code: str,
        start_date: str,
        end_date: str,
    ) -> list[DartFiling]:
        filings: list[DartFiling] = []
        page_no = 1

        while True:
            params = {
                "crtfc_key": self.api_key,
                "corp_code": corp_code,
                "bgn_de": start_date,
                "end_de": end_date,
                "last_reprt_at": "Y",
                "pblntf_ty": "A",
                "sort": "date",
                "sort_mth": "asc",
                "page_count": "100",
                "page_no": str(page_no),
            }
            response = self._get(DISCLOSURE_LIST_URL, params=params)
            response.raise_for_status()

            payload = response.json()
            status = payload.get("status")
            if status == "013":
                break
            if status != "000":
                raise RuntimeError(f"DART list API error {status}: {payload.get('message')}")

            rows = payload.get("list", [])
            for row in rows:
                report_name = str(row.get("report_nm", "")).strip()
                matched_report_code = _infer_report_code(report_name)
                if matched_report_code not in REGULAR_REPORT_CODES:
                    continue
                filings.append(
                    DartFiling(
                        corp_code=corp_code,
                        corp_name=str(row.get("corp_name", "")).strip(),
                        stock_code=str(row.get("stock_code", "")).strip(),
                        report_code=matched_report_code,
                        report_label=REGULAR_REPORT_CODES[matched_report_code],
                        report_name=report_name,
                        receipt_no=str(row.get("rcept_no", "")).strip(),
                        filing_date=str(row.get("rcept_dt", "")).strip(),
                    )
                )

            total_count = int(payload.get("total_count", 0) or 0)
            if page_no * 100 >= total_count or not rows:
                break
            page_no += 1

        return filings

    def download_original_document(self, receipt_no: str) -> dict[str, str]:
        if self.document_source == "html":
            return self.download_original_document_html(receipt_no)
        return self.download_original_document_api(receipt_no)

    def download_original_document_api(self, receipt_no: str) -> dict[str, str]:
        response = self._get(
            DOCUMENT_URL,
            params={
                "crtfc_key": self.api_key,
                "rcept_no": receipt_no,
            },
        )
        response.raise_for_status()

        files: dict[str, str] = {}
        with ZipFile(BytesIO(response.content)) as archive:
            for name in archive.namelist():
                if not _is_candidate_document(name):
                    continue
                raw_bytes = archive.read(name)
                text = _decode_bytes(raw_bytes)
                if text:
                    files[name] = text
        return files

    def download_original_document_html(self, receipt_no: str) -> dict[str, str]:
        nodes = self.load_viewer_nodes(receipt_no)
        files: dict[str, str] = {}

        for index, node in enumerate(nodes, 1):
            cache_path = self._viewer_cache_path(node)
            if cache_path.exists():
                text = cache_path.read_text(encoding="utf-8")
            else:
                response = self._get(
                    VIEWER_URL,
                    params={
                        "rcpNo": node.receipt_no,
                        "dcmNo": node.document_no,
                        "eleId": node.element_id,
                        "offset": node.offset,
                        "length": node.length,
                        "dtd": node.dtd,
                    },
                    headers={"Referer": f"{DISCLOSURE_MAIN_URL}?rcpNo={receipt_no}"},
                    retries=5,
                    html_request=True,
                )
                response.raise_for_status()
                text = response.text
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(text, encoding="utf-8")
            if text:
                file_name = f"{index:03d}_{_sanitize_file_name(node.title)}.html"
                files[file_name] = text

        if not files:
            raise RuntimeError(f"No viewer documents found for receipt {receipt_no}.")
        return files

    def load_viewer_nodes(self, receipt_no: str) -> list[DartViewerNode]:
        cache_path = self._main_cache_path(receipt_no)
        if cache_path.exists():
            content = cache_path.read_text(encoding="utf-8")
        else:
            response = self._get(
                DISCLOSURE_MAIN_URL,
                params={"rcpNo": receipt_no},
                retries=5,
                html_request=True,
            )
            response.raise_for_status()
            content = response.text
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(content, encoding="utf-8")
        nodes = _parse_viewer_nodes(content)
        if not nodes:
            raise RuntimeError(f"Could not parse viewer nodes for receipt {receipt_no}.")
        return [node for node in nodes if _is_relevant_viewer_node(node)]

    def _get(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        retries: int = 2,
        html_request: bool = False,
    ) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                if html_request:
                    self._wait_for_html_slot()
                return self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as error:
                last_error = error
                if attempt == retries - 1:
                    break
                time.sleep((self.html_request_interval if html_request else 0.8) * (attempt + 1))
        assert last_error is not None
        raise last_error

    def _wait_for_html_slot(self) -> None:
        if self.html_request_interval <= 0:
            return
        now = time.monotonic()
        remaining = self.html_request_interval - (now - self._last_html_request_at)
        if remaining > 0:
            time.sleep(remaining)
        self._last_html_request_at = time.monotonic()

    def _main_cache_path(self, receipt_no: str) -> Path:
        return self.cache_dir / "html" / receipt_no / "main.html"

    def _viewer_cache_path(self, node: DartViewerNode) -> Path:
        file_name = (
            f"{node.element_id}_{node.offset}_{node.length}_{_sanitize_file_name(node.title)}.html"
        )
        return self.cache_dir / "html" / node.receipt_no / file_name


def extract_order_backlog_matches(filing: DartFiling, files: dict[str, str]) -> list[OrderBacklogMatch]:
    matches: list[OrderBacklogMatch] = []
    seen_keys: set[tuple[str, str, str | None, str | None]] = set()

    for file_name, content in files.items():
        for section_match in _extract_from_sales_and_orders_section(content):
            key = (file_name, section_match.raw_value or "", section_match.matched_text, section_match.unit)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            matches.append(
                OrderBacklogMatch(
                    receipt_no=filing.receipt_no,
                    filing_date=filing.filing_date,
                    report_name=filing.report_name,
                    report_label=filing.report_label,
                    source_file=file_name,
                    matched_text=section_match.matched_text,
                    raw_value=section_match.raw_value,
                    unit=section_match.unit,
                    amount_krw=section_match.amount_krw,
                    source_kind=section_match.source_kind,
                )
            )

        for xml_table_match in _extract_from_xml_tables(content):
            key = (file_name, xml_table_match.raw_value or "", xml_table_match.matched_text, xml_table_match.unit)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            matches.append(
                OrderBacklogMatch(
                    receipt_no=filing.receipt_no,
                    filing_date=filing.filing_date,
                    report_name=filing.report_name,
                    report_label=filing.report_label,
                    source_file=file_name,
                    matched_text=xml_table_match.matched_text,
                    raw_value=xml_table_match.raw_value,
                    unit=xml_table_match.unit,
                    amount_krw=xml_table_match.amount_krw,
                    source_kind=xml_table_match.source_kind,
                )
            )

        for table_match in _extract_from_tables(content):
            key = (file_name, table_match.raw_value or "", table_match.matched_text, table_match.unit)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            matches.append(
                OrderBacklogMatch(
                    receipt_no=filing.receipt_no,
                    filing_date=filing.filing_date,
                    report_name=filing.report_name,
                    report_label=filing.report_label,
                    source_file=file_name,
                    matched_text=table_match.matched_text,
                    raw_value=table_match.raw_value,
                    unit=table_match.unit,
                    amount_krw=table_match.amount_krw,
                    source_kind=table_match.source_kind,
                )
            )

        for snippet in _extract_text_snippets(content):
            key = (file_name, snippet.raw_value or "", snippet.matched_text, snippet.unit)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            matches.append(
                OrderBacklogMatch(
                    receipt_no=filing.receipt_no,
                    filing_date=filing.filing_date,
                    report_name=filing.report_name,
                    report_label=filing.report_label,
                    source_file=file_name,
                    matched_text=snippet.matched_text,
                    raw_value=snippet.raw_value,
                    unit=snippet.unit,
                    amount_krw=snippet.amount_krw,
                    source_kind=snippet.source_kind,
                )
            )

    matches = _fill_missing_units(matches)

    return sorted(
        matches,
        key=lambda item: (
            item.filing_date,
            item.report_label,
            item.amount_krw is None,
            -(item.amount_krw or 0),
        ),
    )


def _fill_missing_units(matches: list[OrderBacklogMatch]) -> list[OrderBacklogMatch]:
    observed_units = {match.unit for match in matches if match.unit}
    if len(observed_units) != 1:
        return matches

    inferred_unit = next(iter(observed_units))
    normalized_matches: list[OrderBacklogMatch] = []
    for match in matches:
        if match.unit:
            normalized_matches.append(match)
            continue
        normalized_matches.append(
            OrderBacklogMatch(
                receipt_no=match.receipt_no,
                filing_date=match.filing_date,
                report_name=match.report_name,
                report_label=match.report_label,
                source_file=match.source_file,
                matched_text=match.matched_text,
                raw_value=match.raw_value,
                unit=inferred_unit,
                amount_krw=_normalize_amount(match.raw_value, inferred_unit),
                source_kind=match.source_kind,
            )
        )
    return normalized_matches


def matches_to_markdown(company: DartCompany, matches: list[OrderBacklogMatch]) -> str:
    lines = [f"# {company.corp_name} 수주잔고 리포트", ""]
    lines.append(f"- 기업코드: `{company.corp_code}`")
    lines.append(f"- 종목코드: `{company.stock_code or '-'}`")
    lines.append(f"- 추출 건수: `{len(matches)}`")
    lines.append("")

    if not matches:
        lines.append("수주잔고 관련 문구를 찾지 못했습니다.")
        return "\n".join(lines)

    df = pd.DataFrame([match.__dict__ for match in matches])
    df["amount_eok"] = df["amount_krw"].map(_to_eok_value)
    df["amount_display"] = df["amount_eok"].map(_format_eok)
    total_df = build_total_summary(df)

    if not total_df.empty:
        lines.append("## 합계 요약")
        lines.append("")
        lines.append("| 공시일 | 보고서 | 합계 수주잔고(억원) |")
        lines.append("| --- | --- | ---: |")
        for _, row in total_df.iterrows():
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row["filing_date"]),
                        str(row["report_name"]),
                        str(row["amount_display"]),
                    ]
                )
                + " |"
            )
        lines.append("")

    lines.append("## 요약")
    lines.append("")
    lines.append("| 공시일 | 보고서 | 원문 파일 | 추출값(억원) | 원본 단위 |")
    lines.append("| --- | --- | --- | ---: | --- |")
    for _, row in df.iterrows():
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["filing_date"]),
                    str(row["report_name"]),
                    str(row["source_file"]),
                    str(row["amount_display"]),
                    str(row["unit"] or "-"),
                ]
            )
            + " |"
        )

    lines.append("")
    lines.append("## 문맥")
    lines.append("")
    for _, row in df.iterrows():
        lines.append(f"### {row['filing_date']} {row['report_name']}")
        lines.append(f"- 파일: `{row['source_file']}`")
        lines.append(f"- 추출값: `{row['amount_display']}` 억원 / 원본 단위: `{row['unit'] or '-'}`")
        lines.append(f"- 문맥: {row['matched_text']}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def build_total_summary(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = [
        "filing_date",
        "report_name",
        "report_period",
        "amount_display",
        "amount_krw",
        "amount_eok",
        "raw_value",
        "unit",
        "matched_text",
        "source_kind",
    ]
    if df.empty or "matched_text" not in df.columns:
        return pd.DataFrame(columns=expected_columns)

    df = df.copy()
    if "amount_eok" not in df.columns:
        df["amount_eok"] = df["amount_krw"].map(_to_eok_value)
    if "amount_display" not in df.columns:
        df["amount_display"] = df["amount_eok"].map(_format_eok)
    df["source_priority"] = df["source_kind"].map(_source_priority)

    matched_text = df["matched_text"].fillna("")
    total_terms = [
        "수주잔고 금액",
        "합계",
        "총계",
        "기말수주잔고",
        "수주잔고 총액",
        "총 수주잔고",
        "계약잔액",
    ]
    total_mask = matched_text.map(lambda text: any(term in text for term in total_terms))
    total_df = df.loc[total_mask & df["amount_krw"].notna()].copy()

    if total_df.empty:
        table_backed = df.loc[df["source_priority"] <= 2].copy()
        amount_like = table_backed.loc[~table_backed["matched_text"].fillna("").str.contains("수량", na=False)].copy()
        amount_like = amount_like.dropna(subset=["amount_krw"])
        if not amount_like.empty:
            total_df = amount_like.groupby(["filing_date", "report_name"], as_index=False)["amount_krw"].sum()
            total_df["amount_eok"] = total_df["amount_krw"].map(_to_eok_value)
            total_df["amount_display"] = total_df["amount_eok"].map(_format_eok)
            total_df["report_period"] = total_df["report_name"].map(_extract_report_period)
            total_df["raw_value"] = None
            total_df["unit"] = None
            total_df["matched_text"] = None
            total_df["source_kind"] = "aggregated"
            return total_df[expected_columns]

        row_counts = df.groupby(["filing_date", "report_name"]).size().rename("row_count").reset_index()
        single_rows = row_counts.loc[row_counts["row_count"] == 1, ["filing_date", "report_name"]]
        total_df = df.merge(single_rows, on=["filing_date", "report_name"], how="inner")
        total_df = total_df.loc[total_df["source_priority"] <= 2].copy()
        if total_df.empty:
            return pd.DataFrame(columns=expected_columns)
    else:
        table_backed = total_df.loc[total_df["source_priority"] <= 2].copy()
        if not table_backed.empty:
            total_df = table_backed

    conflict_columns = ["filing_date", "report_name", "source_file", "matched_text", "raw_value"]
    if all(column in total_df.columns for column in conflict_columns):
        total_df = total_df.sort_values(
            ["source_priority", "filing_date", "report_name", "amount_krw"],
            ascending=[True, True, True, False],
        )
        total_df = total_df.drop_duplicates(subset=conflict_columns, keep="first")

    total_df["keyword_priority"] = total_df["matched_text"].fillna("").map(_total_keyword_priority)
    total_df = total_df.sort_values(
        ["filing_date", "keyword_priority", "source_priority", "amount_krw"],
        ascending=[True, True, True, False],
    )
    total_df = total_df.drop_duplicates(subset=["filing_date", "report_name"], keep="first")
    total_df["amount_eok"] = total_df["amount_krw"].map(_to_eok_value)
    total_df["amount_display"] = total_df["amount_eok"].map(_format_eok)
    total_df["report_period"] = total_df["report_name"].map(_extract_report_period)
    return total_df[expected_columns]
def batch_totals_to_markdown(results: list[tuple[DartCompany, pd.DataFrame]]) -> str:
    lines = ["# 기업별 수주잔고 합계 비교", ""]
    lines.append(f"- 대상 기업 수: `{len(results)}`")
    lines.append("")

    matrix_rows: list[dict[str, str]] = []
    period_order: list[str] = []

    for company, total_df in results:
        row = {
            "company_name": company.corp_name,
            "stock_code": company.stock_code or "-",
        }
        if total_df.empty:
            row["status"] = "데이터 없음"
        else:
            row["status"] = "OK"
            for _, record in total_df.iterrows():
                period = str(record.get("report_period") or record.get("filing_date"))
                row[period] = str(record["amount_display"])
                if period not in period_order:
                    period_order.append(period)
        matrix_rows.append(row)

    columns = ["company_name", "stock_code", *period_order, "status"]
    lines.append("## 비교표")
    lines.append("")
    if period_order:
        lines.append("| 기업명 | 종목코드 | " + " | ".join(period_order) + " | 상태 |")
        lines.append("| --- | --- | " + " | ".join(["---:" for _ in period_order]) + " | --- |")
    else:
        lines.append("| 기업명 | 종목코드 | 상태 |")
        lines.append("| --- | --- | --- |")
    for row in matrix_rows:
        values = [row.get("company_name", "-"), row.get("stock_code", "-")]
        if period_order:
            values.extend(row.get(period, "-") for period in period_order)
        values.append(row.get("status", "-"))
        lines.append("| " + " | ".join(values) + " |")

    lines.append("")
    lines.append("## 기준")
    lines.append("")
    lines.append("- 값은 각 보고서의 총 수주잔고 후보값을 `억원` 기준으로 환산한 값입니다.")
    lines.append("- 총액 후보는 `합계`, `총계`, `기말수주잔고`, `수주잔고 총액` 등의 문맥을 우선 사용합니다.")
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _infer_report_code(report_name: str) -> str | None:
    if "분기보고서" in report_name:
        if any(token in report_name for token in ["09", "9월", "3분기"]):
            return "11014"
        return "11013"
    if "반기보고서" in report_name:
        return "11012"
    if "사업보고서" in report_name:
        return "11011"
    return None


def _parse_viewer_nodes(content: str) -> list[DartViewerNode]:
    block_pattern = re.compile(
        r"var\s+(node(?P<level>\d+))\s*=\s*\{\};(?P<body>.*?)(?=(?:\bvar\s+node\d+\s*=\s*\{\};)|\Z)",
        re.DOTALL,
    )
    nodes: list[DartViewerNode] = []
    seen_keys: set[tuple[str, str, str]] = set()

    for match in block_pattern.finditer(content):
        variable_name = match.group(1)
        level = int(match.group("level"))
        body = match.group("body")
        field_pattern = re.compile(
            rf"{re.escape(variable_name)}\['(?P<key>[^']+)'\]\s*=\s*\"(?P<value>(?:\\.|[^\"])*)\";",
            re.DOTALL,
        )
        fields = {
            field_match.group("key"): _decode_js_string(field_match.group("value"))
            for field_match in field_pattern.finditer(body)
        }
        required_fields = {"text", "rcpNo", "dcmNo", "eleId", "offset", "length", "dtd"}
        if not required_fields.issubset(fields):
            continue

        key = (fields["eleId"], fields["offset"], fields["length"])
        if key in seen_keys:
            continue
        seen_keys.add(key)
        nodes.append(
            DartViewerNode(
                title=fields["text"],
                level=level,
                receipt_no=fields["rcpNo"],
                document_no=fields["dcmNo"],
                element_id=fields["eleId"],
                offset=fields["offset"],
                length=fields["length"],
                dtd=fields["dtd"],
                toc_no=fields.get("tocNo", ""),
            )
        )

    return nodes


def _decode_js_string(value: str) -> str:
    return value.replace(r"\/", "/").replace(r"\'", "'").replace(r"\"", '"').replace(r"\n", "\n").strip()


def _is_relevant_viewer_node(node: DartViewerNode) -> bool:
    return HTML_TARGET_SECTION_PATTERN.search(node.title) is not None


def _sanitize_file_name(value: str) -> str:
    sanitized = re.sub(r'[\\/:*?"<>|]+', "_", value)
    sanitized = re.sub(r"\s+", "_", sanitized).strip("._")
    return sanitized or "document"


def _is_candidate_document(file_name: str) -> bool:
    lowered = file_name.lower()
    return lowered.endswith((".xml", ".htm", ".html", ".xhtml", ".txt"))


def _decode_bytes(raw_bytes: bytes) -> str:
    for encoding in ("utf-8", "cp949", "euc-kr", "utf-16"):
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("utf-8", errors="ignore")


def _extract_from_tables(content: str) -> list[OrderBacklogMatch]:
    matches: list[OrderBacklogMatch] = []
    current_unit: str | None = None
    for table_match in re.finditer(r"(<table\b.*?</table>)", content, flags=re.DOTALL | re.IGNORECASE):
        table_block = table_match.group(1)
        tables = _read_html_tables(table_block)
        if not tables:
            continue
        table = tables[0]
        normalized_rows = table.fillna("").astype(str).apply(lambda column: column.map(_compact_text)).values.tolist()
        if not normalized_rows:
            continue
        context_start = max(table_match.start() - 3000, 0)
        prior_context = content[context_start:table_match.start()]
        unit = _infer_backlog_unit(
            content,
            table_context=table_block,
            table_text=" ".join(cell for row in normalized_rows for cell in row if cell),
            local_context=prior_context,
            inherited_unit=current_unit,
        )
        if unit:
            current_unit = unit
        matches.extend(
            _extract_backlog_matches_from_rows(
                content,
                normalized_rows,
                source_kind="table",
                table_context=table_block,
                local_context=prior_context,
                inherited_unit=unit,
            )
        )

    return matches


def _read_html_tables(content: str) -> list[pd.DataFrame]:
    try:
        return pd.read_html(StringIO(content))
    except Exception:
        pass

    parsed_rows = _parse_html_tables(content)
    dataframes: list[pd.DataFrame] = []
    for rows in parsed_rows:
        if not rows:
            continue
        max_columns = max(len(row) for row in rows)
        normalized_rows = [row + [""] * (max_columns - len(row)) for row in rows]
        dataframes.append(pd.DataFrame(normalized_rows))
    return dataframes


class _SimpleHtmlTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[tuple[str, dict[str, str]]]]] = []
        self._table_depth = 0
        self._current_table: list[list[tuple[str, dict[str, str]]]] | None = None
        self._current_row: list[tuple[str, dict[str, str]]] | None = None
        self._current_cell: list[str] | None = None
        self._current_attrs: dict[str, str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        if lowered == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self._current_table = []
            return
        if self._table_depth == 0:
            return
        if lowered == "tr":
            self._current_row = []
            return
        if lowered in {"td", "th"}:
            self._current_cell = []
            self._current_attrs = {key.lower(): value or "" for key, value in attrs}
            return
        if lowered == "br" and self._current_cell is not None:
            self._current_cell.append("\n")

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if self._table_depth == 0:
            return
        if lowered in {"td", "th"} and self._current_cell is not None and self._current_row is not None:
            text = _compact_text("".join(self._current_cell))
            self._current_row.append((text, self._current_attrs or {}))
            self._current_cell = None
            self._current_attrs = None
            return
        if lowered == "tr" and self._current_row is not None and self._current_table is not None:
            self._current_table.append(self._current_row)
            self._current_row = None
            return
        if lowered == "table":
            if self._table_depth == 1 and self._current_table is not None:
                self.tables.append(self._current_table)
                self._current_table = None
            self._table_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell.append(data)


def _parse_html_tables(content: str) -> list[list[list[str]]]:
    parser = _SimpleHtmlTableParser()
    parser.feed(content)
    parsed_tables: list[list[list[str]]] = []
    for table_rows in parser.tables:
        grid: list[list[str]] = []
        spanning: dict[tuple[int, int], str] = {}
        max_columns = 0
        for row_index, row in enumerate(table_rows):
            current_row: list[str] = []
            column_index = 0
            while (row_index, column_index) in spanning:
                current_row.append(spanning[(row_index, column_index)])
                column_index += 1

            for text, attrs in row:
                while (row_index, column_index) in spanning:
                    current_row.append(spanning[(row_index, column_index)])
                    column_index += 1

                colspan = int(attrs.get("colspan", "1") or "1")
                rowspan = int(attrs.get("rowspan", "1") or "1")
                for offset in range(colspan):
                    current_row.append(text)
                    if rowspan > 1:
                        for future_row in range(1, rowspan):
                            spanning[(row_index + future_row, column_index + offset)] = text
                column_index += colspan

            while (row_index, column_index) in spanning:
                current_row.append(spanning[(row_index, column_index)])
                column_index += 1

            max_columns = max(max_columns, len(current_row))
            grid.append(current_row)

        parsed_tables.append([row + [""] * (max_columns - len(row)) for row in grid])
    return parsed_tables


def _extract_text_snippets(content: str) -> list[OrderBacklogMatch]:
    text = _html_to_text(content)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    snippets: list[OrderBacklogMatch] = []

    for index, line in enumerate(lines):
        if not any(pattern.search(line) for pattern in ORDER_BACKLOG_PATTERNS):
            continue
        start = max(index - 1, 0)
        end = min(index + 2, len(lines))
        snippet = " ".join(lines[start:end])
        if _is_negative_backlog_context_safe(snippet):
            continue
        if _contains_foreign_currency_marker(snippet):
            continue
        number = _pick_largest_number(snippet)
        if number is None:
            continue
        unit = _infer_backlog_unit(content, table_context=snippet, table_text=snippet, local_context=snippet)
        snippets.append(
            OrderBacklogMatch(
                receipt_no="",
                filing_date="",
                report_name="",
                report_label="",
                source_file="",
                matched_text=_compact_text(snippet),
                raw_value=number,
                unit=unit,
                amount_krw=_normalize_amount(number, unit) if number else None,
                source_kind="snippet",
            )
        )

    summary_match = _extract_backlog_summary_from_text(
        text,
        lines,
        _infer_backlog_unit(content, table_context=text, table_text=text, local_context=text),
    )
    if summary_match is not None:
        if not any(
            item.raw_value == summary_match.raw_value and item.matched_text == summary_match.matched_text
            for item in snippets
        ):
            snippets.append(summary_match)

    return snippets


def _is_negative_backlog_context(text: str) -> bool:
    normalized = _compact_text(text)
    negative_phrases = [
        "해당사항이없습니다",
        "해당사항없습니다",
        "수주상황이없습니다",
        "수주잔고가없습니다",
        "수주잔고는없습니다",
        "수주잔고를정확히산출하기는어렵습니다",
        "수주총액과수주잔고를정확히산출하기는어렵습니다",
        "장기간의수주총액과수주잔고를정확히산출하기는어렵습니다",
        "정확히산출하기는어렵습니다",
        "산출하기어렵습니다",
    ]
    return any(phrase in normalized for phrase in negative_phrases)


def _is_negative_backlog_context_safe(text: str) -> bool:
    normalized = _compact_text(text)
    negative_phrases = [
        "\ud574\ub2f9\uc0ac\ud56d\uc774\uc5c6\uc2b5\ub2c8\ub2e4",
        "\ud574\ub2f9\uc0ac\ud56d\uc5c6\uc2b5\ub2c8\ub2e4",
        "\uc218\uc8fc\uc0c1\ud669\uc774\uc5c6\uc2b5\ub2c8\ub2e4",
        "\uc218\uc8fc\uc794\uace0\uac00\uc5c6\uc2b5\ub2c8\ub2e4",
        "\uc218\uc8fc\uc794\uace0\ub294\uc5c6\uc2b5\ub2c8\ub2e4",
        "\uc218\uc8fc\uc794\uace0\ub97c\uc815\ud655\ud788\uc0b0\ucd9c\ud558\uae30\ub294\uc5b4\ub835\uc2b5\ub2c8\ub2e4",
        "\uc218\uc8fc\ucd1d\uc561\uacfc\uc218\uc8fc\uc794\uace0\ub97c\uc815\ud655\ud788\uc0b0\ucd9c\ud558\uae30\ub294\uc5b4\ub835\uc2b5\ub2c8\ub2e4",
        "\uc7a5\uae30\uac04\uc758\uc218\uc8fc\ucd1d\uc561\uacfc\uc218\uc8fc\uc794\uace0\ub97c\uc815\ud655\ud788\uc0b0\ucd9c\ud558\uae30\ub294\uc5b4\ub835\uc2b5\ub2c8\ub2e4",
        "\uc815\ud655\ud788\uc0b0\ucd9c\ud558\uae30\ub294\uc5b4\ub835\uc2b5\ub2c8\ub2e4",
        "\uc0b0\ucd9c\ud558\uae30\uc5b4\ub835\uc2b5\ub2c8\ub2e4",
    ]
    if any(phrase in normalized for phrase in negative_phrases):
        return True

    no_applicable = "\ud574\ub2f9\uc0ac\ud56d" in normalized and "\uc5c6\uc2b5\ub2c8\ub2e4" in normalized
    backlog_not_available = "\uc218\uc8fc\uc0c1\ud669" in normalized and "\uc5c6\uc2b5\ub2c8\ub2e4" in normalized
    backlog_hard_to_measure = (
        "\uc218\uc8fc\uc794\uace0" in normalized
        and "\uc0b0\ucd9c" in normalized
        and "\uc5b4\ub835" in normalized
    )
    return no_applicable or backlog_not_available or backlog_hard_to_measure


def _extract_backlog_summary_from_text(text: str, lines: list[str], unit: str | None) -> OrderBacklogMatch | None:
    normalized_text = _compact_text(text)
    if _is_negative_backlog_context_safe(normalized_text):
        return None
    if _contains_foreign_currency_marker(text):
        return None
    if "수주잔고" in normalized_text:
        section_text = normalized_text[normalized_text.rfind("수주잔고") :]
        total_match = re.search(r"합\s*계(?P<body>.*)", section_text)
        if total_match:
            body = total_match.group("body")
            body = re.split(r"\(\*\d+\)|[가-힣]\.", body, maxsplit=1)[0]
            numbers = [match.group(1) for match in NUMBER_PATTERN.finditer(body)]
            if numbers:
                raw_value = numbers[-1]
                return OrderBacklogMatch(
                    receipt_no="",
                    filing_date="",
                    report_name="",
                    report_label="",
                    source_file="",
                    matched_text=_compact_text(f"합계 | 수주잔고 | {raw_value}"),
                    raw_value=raw_value,
                    unit=unit,
                    amount_krw=_normalize_amount(raw_value, unit),
                    source_kind="snippet",
                )

    normalized_lines = [_compact_text(line) for line in lines]
    order_status_indices = [index for index, line in enumerate(normalized_lines) if "수주현황" in line]
    if not order_status_indices:
        return None
    if not any("수주잔고" in line for line in normalized_lines):
        return None
    search_start = order_status_indices[-1]
    if any(_is_negative_backlog_context_safe(line) for line in normalized_lines[search_start:]):
        return None

    for index, line in enumerate(normalized_lines[search_start:], start=search_start):
        compact = line.replace(" ", "")
        if compact not in {"합계", "합계-", "합계:"} and "합계" not in compact:
            continue
        numbers: list[str] = []
        for next_line in normalized_lines[index + 1 :]:
            if next_line.startswith("(*"):
                break
            cleaned = _clean_numeric_text(next_line.strip("()"))
            if cleaned is not None:
                numbers.append(cleaned)
            if len(numbers) >= 6:
                break
        if not numbers:
            continue
        raw_value = numbers[-1]
        return OrderBacklogMatch(
            receipt_no="",
            filing_date="",
            report_name="",
            report_label="",
            source_file="",
            matched_text=_compact_text(f"합계 | 수주잔고 | {raw_value}"),
            raw_value=raw_value,
            unit=unit,
            amount_krw=_normalize_amount(raw_value, unit),
            source_kind="snippet",
        )
    return None


def _html_to_text(content: str) -> str:
    normalized = content.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    normalized = re.sub(r"</(p|tr|div|li|table|h\d)>", "\n", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"</(td|th)>", "\t", normalized, flags=re.IGNORECASE)
    normalized = TAG_PATTERN.sub(" ", normalized)
    normalized = normalized.replace("&nbsp;", " ")
    normalized = WHITESPACE_PATTERN.sub(" ", normalized)
    return normalized


def _compact_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _source_priority(source_kind: str | None) -> int:
    return SOURCE_PRIORITY.get(source_kind or "generic", 9)


def _infer_backlog_unit(
    content: str,
    *,
    table_context: str = "",
    table_text: str = "",
    local_context: str = "",
    inherited_unit: str | None = None,
) -> str | None:
    explicit_unit = _detect_explicit_backlog_unit(local_context) or _detect_explicit_backlog_unit(table_context)
    return (
        explicit_unit
        or _detect_nearest_unit(local_context, loose=False)
        or _detect_nearest_unit(table_context, loose=True)
        or _detect_unit(table_text, loose=False)
        or inherited_unit
        or _detect_document_unit(content)
    )


def _detect_explicit_backlog_unit(text: str) -> str | None:
    normalized = _compact_text(text)
    marker = "\ub2e8\uc704"
    units = ["\ubc31\ub9cc\uc6d0", "\ucc9c\uc6d0", "\uc5b5\uc6d0", "\ub9cc\uc6d0", "\uc6d0"]

    marker_index = normalized.find(marker)
    if marker_index < 0:
        return None

    window = normalized[marker_index : marker_index + 120]
    for unit in units:
        if unit in window:
            return unit
    return None


def _extract_backlog_matches_from_rows(
    content: str,
    normalized_rows: list[list[str]],
    *,
    source_kind: str,
    table_context: str = "",
    local_context: str = "",
    inherited_unit: str | None = None,
) -> list[OrderBacklogMatch]:
    if not normalized_rows:
        return []

    table_text = " ".join(cell for row in normalized_rows for cell in row if cell)
    unit = _infer_backlog_unit(
        content,
        table_context=table_context or table_text,
        table_text=table_text,
        local_context=local_context or table_text,
        inherited_unit=inherited_unit,
    )
    backlog_columns = _find_backlog_columns(normalized_rows)
    if not backlog_columns:
        return []

    header_rows = _estimate_header_row_count(normalized_rows)
    matches: list[OrderBacklogMatch] = []
    for row in normalized_rows[header_rows:]:
        if len(row) <= max(backlog_columns):
            continue
        label = _build_row_label(row, backlog_columns[0])
        for backlog_col in backlog_columns:
            raw_value = _clean_numeric_text(row[backlog_col])
            if raw_value is None:
                continue
            header = _column_header_text(normalized_rows[:header_rows], backlog_col)
            matched_text = _compact_text(f"{label} | {header} | {raw_value}")
            if _contains_foreign_currency_marker(matched_text):
                continue
            matches.append(
                OrderBacklogMatch(
                    receipt_no="",
                    filing_date="",
                    report_name="",
                    report_label="",
                    source_file="",
                    matched_text=matched_text,
                    raw_value=raw_value,
                    unit=unit,
                    amount_krw=_normalize_amount(raw_value, unit),
                    source_kind=source_kind,
                )
            )
    return matches


def _extract_from_sales_and_orders_section(content: str) -> list[OrderBacklogMatch]:
    section = _find_sales_and_orders_section(content)
    if section is None:
        return []

    section_text = " ".join(section.itertext())
    section_unit = _infer_backlog_unit(content, table_context=section_text, table_text=section_text, local_context=section_text)
    matches: list[OrderBacklogMatch] = []

    for table in section.iter("TABLE"):
        grid = _table_to_grid(table)
        if not grid:
            continue
        normalized_rows = [[_compact_text(cell) for cell in row] for row in grid]
        matches.extend(
            _extract_backlog_matches_from_rows(
                content,
                normalized_rows,
                source_kind="section_table",
                table_context=section_text,
                local_context=section_text,
                inherited_unit=section_unit,
            )
        )

    return matches


def _extract_from_xml_tables(content: str) -> list[OrderBacklogMatch]:
    matches: list[OrderBacklogMatch] = []
    current_unit: str | None = None
    for table_match in re.finditer(r"(<TABLE\b.*?</TABLE>)", content, flags=re.DOTALL | re.IGNORECASE):
        table_block = table_match.group(1)
        try:
            table = ET.fromstring(table_block)
        except ET.ParseError:
            continue

        grid = _table_to_grid(table)
        if not grid:
            continue
        normalized_rows = [[_compact_text(cell) for cell in row] for row in grid]
        context_start = max(table_match.start() - 3000, 0)
        prior_context = content[context_start:table_match.start()]
        unit = _infer_backlog_unit(
            content,
            table_context=table_block,
            table_text=" ".join(cell for row in normalized_rows for cell in row if cell),
            local_context=prior_context,
            inherited_unit=current_unit,
        )
        if unit:
            current_unit = unit
        matches.extend(
            _extract_backlog_matches_from_rows(
                content,
                normalized_rows,
                source_kind="xml_table",
                table_context=table_block,
                local_context=prior_context,
                inherited_unit=unit,
            )
        )
    return matches


def _find_sales_and_orders_section(content: str) -> ET.Element | None:
    for block in _extract_section_blocks(content):
        if "매출 및 수주상황" not in block:
            continue
        try:
            section = ET.fromstring(block)
        except ET.ParseError:
            continue
        titles = [
            _compact_text("".join(title.itertext()))
            for title in section.iter()
            if _local_name(title.tag) == "TITLE"
        ]
        if any("매출 및 수주상황" in title for title in titles):
            return section
    return None


def _table_to_grid(table: ET.Element) -> list[list[str]]:
    rows: list[list[str]] = []
    spanning: dict[tuple[int, int], str] = {}
    max_columns = 0

    tr_elements = [child for child in table.iter() if _local_name(child.tag) == "TR"]
    for row_index, tr in enumerate(tr_elements):
        row: list[str] = []
        column_index = 0

        while (row_index, column_index) in spanning:
            row.append(spanning[(row_index, column_index)])
            column_index += 1

        cells = [child for child in tr if _local_name(child.tag) in {"TD", "TH"}]
        for cell in cells:
            while (row_index, column_index) in spanning:
                row.append(spanning[(row_index, column_index)])
                column_index += 1

            text = _compact_text("".join(cell.itertext()))
            colspan = int(cell.attrib.get("COLSPAN", cell.attrib.get("colspan", "1")) or "1")
            rowspan = int(cell.attrib.get("ROWSPAN", cell.attrib.get("rowspan", "1")) or "1")

            for offset in range(colspan):
                row.append(text)
                if rowspan > 1:
                    for future_row in range(1, rowspan):
                        spanning[(row_index + future_row, column_index + offset)] = text
            column_index += colspan

        while (row_index, column_index) in spanning:
            row.append(spanning[(row_index, column_index)])
            column_index += 1

        max_columns = max(max_columns, len(row))
        rows.append(row)

    return [row + [""] * (max_columns - len(row)) for row in rows]


def _find_backlog_columns(rows: list[list[str]]) -> list[int]:
    header_rows = rows[: _estimate_header_row_count(rows)]
    backlog_columns: list[int] = []
    if not header_rows:
        return backlog_columns

    for column_index in range(len(header_rows[0])):
        header_text = _column_header_text(header_rows, column_index)
        if any(keyword in header_text for keyword in BACKLOG_HEADER_KEYWORDS):
            backlog_columns.append(column_index)
    if backlog_columns:
        amount_columns = [
            column_index
            for column_index in backlog_columns
            if "금액" in _column_header_text(header_rows, column_index)
        ]
        if amount_columns:
            return amount_columns

        non_quantity_columns = [
            column_index
            for column_index in backlog_columns
            if "수량" not in _column_header_text(header_rows, column_index)
        ]
        if non_quantity_columns:
            return non_quantity_columns

        return backlog_columns

    full_header_text = _compact_text(" ".join(_column_header_text(header_rows, index) for index in range(len(header_rows[0]))))
    has_contract_balance_shape = all(keyword in full_header_text for keyword in ["기초", "증감", "기말"]) and (
        "계약" in full_header_text or "매출계상액" in full_header_text
    )
    if has_contract_balance_shape:
        for column_index in range(len(header_rows[0])):
            header_text = _column_header_text(header_rows, column_index)
            if "기말" in header_text:
                backlog_columns.append(column_index)
        if backlog_columns:
            return backlog_columns

    # Some companies disclose "주요 수주상황" with headers like
    # 품목 | 내용 | 수주일자(계약일자) | 금액 and a final 합계 row.
    # Treat the amount column as the backlog total candidate in this shape.
    full_header_text = _compact_text(" ".join(_column_header_text(header_rows, index) for index in range(len(header_rows[0]))))
    has_major_order_shape = "수주일자" in full_header_text and "금액" in full_header_text
    if not has_major_order_shape:
        return backlog_columns

    for column_index in range(len(header_rows[0]) - 1, -1, -1):
        header_text = _column_header_text(header_rows, column_index)
        if "금액" in header_text:
            backlog_columns.append(column_index)
            break
    return backlog_columns


def _estimate_header_row_count(rows: list[list[str]]) -> int:
    for index, row in enumerate(rows):
        numeric_cells = sum(1 for cell in row if _clean_numeric_text(cell) is not None)
        if numeric_cells >= 2:
            return max(index, 1)
    if len(rows) >= 2:
        first_row_numeric = sum(1 for cell in rows[0] if _clean_numeric_text(cell) is not None)
        second_row_numeric = sum(1 for cell in rows[1] if _clean_numeric_text(cell) is not None)
        if first_row_numeric == 0 and second_row_numeric >= 1:
            return 1
    return min(len(rows), 2)


def _column_header_text(header_rows: list[list[str]], column_index: int) -> str:
    return _compact_text(" ".join(row[column_index] for row in header_rows if column_index < len(row)))


def _build_row_label(row: list[str], backlog_col: int) -> str:
    candidate_cells = []
    for cell in row[:backlog_col]:
        compact = _compact_text(cell)
        if not compact:
            continue
        if _clean_numeric_text(compact) is not None:
            continue
        candidate_cells.append(compact)
    return " / ".join(candidate_cells) or "-"


def _clean_numeric_text(text: str) -> str | None:
    compact = _compact_text(text)
    if not compact or compact.lower() == "nan":
        return None
    if compact in {"-", "△", "N/A"}:
        return None
    if NUMBER_PATTERN.fullmatch(compact) is None:
        return None
    return compact


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _extract_report_period(report_name: str) -> str:
    match = re.search(r"\((\d{4}\.\d{2})\)", report_name)
    if match:
        return match.group(1)
    if "사업보고서" in report_name:
        return "Annual"
    if "반기보고서" in report_name:
        return "Half"
    if "분기보고서" in report_name:
        return "Quarter"
    return report_name


def _total_keyword_priority(text: str) -> int:
    if any(keyword in text for keyword in ["수주잔고 금액", "기말수주잔고", "수주잔고 총액"]):
        return 0
    if "계약잔액" in text:
        return 1
    if any(keyword in text for keyword in ["합 계", "합계", "총계", "계 |"]):
        return 2
    return 3


def _unit_patterns() -> list[str]:
    return ["십억원", "억원", "천만원", "백만원", "천원", "원"]


def _detect_unit(text: str, loose: bool = False) -> str | None:
    unit_patterns = _unit_patterns()
    for unit in unit_patterns:
        if re.search(rf"\(?\s*단위\s*[:：]\s*{re.escape(unit)}\s*\)?", text):
            return unit
    if loose:
        for unit in unit_patterns:
            if unit in text:
                return unit
    return None


def _detect_nearest_unit(text: str, loose: bool = False) -> str | None:
    unit_patterns = _unit_patterns()
    matches: list[tuple[int, str]] = []
    for unit in unit_patterns:
        for match in re.finditer(rf"\(?\s*단위\s*[:：]\s*{re.escape(unit)}\s*\)?", text):
            matches.append((match.start(), unit))
    if matches:
        matches.sort(key=lambda item: item[0])
        return matches[-1][1]
    if loose:
        for unit in unit_patterns:
            index = text.rfind(unit)
            if index >= 0:
                matches.append((index, unit))
        if matches:
            matches.sort(key=lambda item: item[0])
            return matches[-1][1]
    return None


def _detect_document_unit(content: str) -> str | None:
    return _detect_nearest_unit(content, loose=False) or _detect_nearest_unit(_html_to_text(content), loose=True)


def _contains_foreign_currency_marker(text: str) -> bool:
    upper = text.upper()
    if "$" in text:
        return True
    if "KRW" in upper or "원화" in text:
        return False
    return any(marker in upper for marker in ["JPY", "USD", "EUR", "CNY", "VND", "IDR", "THB", "BAHT"])


def _pick_largest_number(text: str) -> str | None:
    candidates = [
        match.group(1)
        for match in NUMBER_PATTERN.finditer(text)
        if match.group(1).lower() != "nan"
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: float(item.replace(",", "")))


def _normalize_amount(raw_value: str | None, unit: str | None) -> int | None:
    if raw_value is None:
        return None
    try:
        value = float(raw_value.replace(",", ""))
    except ValueError:
        return None
    multiplier = UNIT_MULTIPLIERS.get(unit or "", 1)
    return int(value * multiplier)


def _to_eok_value(amount_krw: int | None) -> float | None:
    if amount_krw is None:
        return None
    return amount_krw / 100_000_000


def _format_eok(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    formatted = f"{value:,.2f}"
    return formatted.rstrip("0").rstrip(".")


def _extract_section_blocks(content: str) -> list[str]:
    pattern = re.compile(r"(<(?P<tag>SECTION(?:-\d+)?)\b[^>]*>.*?</(?P=tag)>)", re.DOTALL)
    return [match.group(1) for match in pattern.finditer(content)]
