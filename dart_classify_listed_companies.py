from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from trade_tracker.config import get_settings
from trade_tracker.dart import DartClient, build_total_summary, extract_order_backlog_matches


load_dotenv()


RESULT_COLUMNS = [
    "corp_code",
    "corp_name",
    "stock_code",
    "start_date",
    "end_date",
    "filings",
    "processed_filings",
    "skipped_filings",
    "match_count",
    "matched_filing_count",
    "total_count",
    "has_backlog_keyword",
    "has_backlog_total",
    "latest_total_period",
    "latest_total_eok",
    "status",
]

CORP_CODE_WIDTH = 8
STOCK_CODE_WIDTH = 6
MANUAL_NO_BACKLOG_STOCK_CODES = {
    "031990",  # 대선조선
    "043360",  # 디지아이
    "106080",  # 케이이엠텍
    "277410",  # 인산가
    "419530",  # SAMG엔터
    "900340",  # 윙입푸드
}
MANUAL_NO_BACKLOG_SIGNAL_COMPANIES = {
    "BNK금융지주",
    "DB손해보험",
    "DB증권",
}


def build_parser() -> argparse.ArgumentParser:
    today = date.today()
    default_end_date = today.strftime("%Y%m%d")
    default_start_date = (today - timedelta(days=365)).strftime("%Y%m%d")
    parser = argparse.ArgumentParser(
        description="Classify listed DART companies by whether their regular filings contain order-backlog disclosures."
    )
    parser.add_argument(
        "--start-date",
        default=default_start_date,
        help=f"Start filing date, e.g. 20240101. Default: {default_start_date}",
    )
    parser.add_argument(
        "--end-date",
        default=default_end_date,
        help=f"End filing date, e.g. 20241231. Default: {default_end_date}",
    )
    parser.add_argument(
        "--output-csv",
        default="outputs/dart_listed_backlog_classification.csv",
        help="CSV output path",
    )
    parser.add_argument(
        "--output-md",
        default="outputs/dart_listed_backlog_classification.md",
        help="Markdown summary output path",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit for testing",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from an existing CSV if present",
    )
    parser.add_argument(
        "--document-source",
        choices=["html", "api"],
        default="api",
        help="How to load filing contents: direct DART HTML viewer or OpenDART document API",
    )
    parser.add_argument(
        "--html-request-interval",
        type=float,
        default=3.0,
        help="Minimum seconds between DART HTML requests when --document-source html is used",
    )
    parser.add_argument(
        "--cache-dir",
        help="Optional cache directory for downloaded DART HTML pages",
    )
    parser.add_argument(
        "--finalize-only",
        action="store_true",
        help="Do not scan DART again. Only normalize the existing CSV and write the markdown summary.",
    )
    parser.add_argument(
        "--scan-mode",
        choices=["latest-only", "full-period"],
        default="latest-only",
        help="Check only the latest regular filing in the date range, or scan every regular filing. Default: latest-only",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    output_csv = Path(args.output_csv)
    output_md = Path(args.output_md)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)

    existing_df = _load_existing_results(output_csv) if args.resume else pd.DataFrame(columns=RESULT_COLUMNS)
    if args.finalize_only:
        result_df = existing_df.sort_values(["has_backlog_total", "has_backlog_keyword", "corp_name"], ascending=[False, False, True])
        _write_csv(output_csv, result_df.to_dict(orient="records"))
        output_md.write_text(_build_markdown_summary(result_df, args.start_date, args.end_date), encoding="utf-8")
        print(f"CSV saved to: {output_csv}")
        print(f"Markdown saved to: {output_md}")
        print(f"Companies with backlog keyword: {int(result_df['has_backlog_keyword'].fillna(False).sum())}")
        print(f"Companies with backlog total: {int(result_df['has_backlog_total'].fillna(False).sum())}")
        return

    settings = get_settings()
    client = DartClient(
        api_key=settings.dart_api_key,
        document_source=args.document_source,
        html_request_interval=args.html_request_interval,
        cache_dir=args.cache_dir,
    )
    companies = [company for company in client.load_corp_codes() if company.stock_code]
    companies = sorted(companies, key=lambda item: (item.corp_name, item.stock_code))
    if args.limit:
        companies = companies[: args.limit]

    processed_corp_codes = set(existing_df["corp_code"].dropna().astype(str)) if not existing_df.empty else set()
    rows: list[dict[str, object]] = existing_df.to_dict(orient="records")

    target_companies = [company for company in companies if company.corp_code not in processed_corp_codes]

    for index, company in enumerate(target_companies, 1):
        try:
            row = _classify_company(
                client,
                company.corp_code,
                company.corp_name,
                company.stock_code,
                args.start_date,
                args.end_date,
                args.scan_mode,
            )
        except RuntimeError as error:
            if "020" in str(error):
                print(f"Stopped due to DART API limit: {error}")
                break
            raise
        rows.append(row)
        _write_csv(output_csv, rows)
        print(
            f"{index:04d}/{len(target_companies):04d} {company.corp_name}({company.stock_code}) "
            f"filings={row['filings']} matches={row['match_count']} totals={row['total_count']} status={row['status']}"
        )

    result_df = _normalize_result_frame(pd.DataFrame(rows, columns=RESULT_COLUMNS))
    result_df = result_df.sort_values(["has_backlog_total", "has_backlog_keyword", "corp_name"], ascending=[False, False, True])
    _write_csv(output_csv, result_df.to_dict(orient="records"))
    output_md.write_text(_build_markdown_summary(result_df, args.start_date, args.end_date), encoding="utf-8")

    print(f"CSV saved to: {output_csv}")
    print(f"Markdown saved to: {output_md}")
    print(f"Companies with backlog keyword: {int(result_df['has_backlog_keyword'].fillna(False).sum())}")
    print(f"Companies with backlog total: {int(result_df['has_backlog_total'].fillna(False).sum())}")


def _classify_company(
    client: DartClient,
    corp_code: str,
    corp_name: str,
    stock_code: str,
    start_date: str,
    end_date: str,
    scan_mode: str,
) -> dict[str, object]:
    filings = client.list_regular_filings(corp_code=corp_code, start_date=start_date, end_date=end_date)
    filings_to_process, no_target_section = _select_filings_to_process(client, filings, scan_mode)
    matches = []
    matched_filing_numbers: set[str] = set()
    skipped_filings = 0
    processed_filings = 0

    for filing in filings_to_process:
        try:
            files = client.download_original_document(filing.receipt_no)
        except Exception:
            skipped_filings += 1
            continue

        processed_filings += 1
        filing_matches = extract_order_backlog_matches(filing, files)
        if filing_matches:
            matched_filing_numbers.add(filing.receipt_no)
            matches.extend(filing_matches)

    matches_df = pd.DataFrame([match.__dict__ for match in matches])
    total_df = build_total_summary(matches_df, stock_code=stock_code)

    latest_total_period = ""
    latest_total_eok: float | None = None
    if not total_df.empty:
        latest_total_row = total_df.sort_values("filing_date").iloc[-1]
        latest_total_period = str(latest_total_row["report_period"])
        latest_total_eok = float(latest_total_row["amount_eok"])

    inferred_status = _infer_status(len(filings_to_process), bool(matches), not total_df.empty, skipped_filings)
    if no_target_section and inferred_status in {"no_regular_filings", "no_match_with_skips"}:
        inferred_status = "no_backlog_signal"

    return {
        "corp_code": corp_code,
        "corp_name": corp_name,
        "stock_code": stock_code,
        "start_date": start_date,
        "end_date": end_date,
        "filings": len(filings_to_process),
        "processed_filings": processed_filings,
        "skipped_filings": skipped_filings,
        "match_count": len(matches),
        "matched_filing_count": len(matched_filing_numbers),
        "total_count": 0 if total_df.empty else len(total_df),
        "has_backlog_keyword": bool(matches),
        "has_backlog_total": not total_df.empty,
        "latest_total_period": latest_total_period,
        "latest_total_eok": latest_total_eok,
        "status": _apply_manual_status_override(corp_name, inferred_status),
    }


def _select_filings_to_process(
    client: DartClient,
    filings: list,
    scan_mode: str,
) -> tuple[list, bool]:
    if scan_mode != "latest-only":
        return filings, False
    if not filings:
        return [], False
    if client.document_source != "html":
        return filings[-1:], False

    for filing in reversed(filings):
        try:
            nodes = client.load_viewer_nodes(filing.receipt_no)
        except Exception:
            return [filings[-1]], False
        if nodes:
            return [filing], False
    return [filings[-1]], True


def _infer_status(filings: int, has_keyword: bool, has_total: bool, skipped_filings: int) -> str:
    if filings == 0:
        return "no_regular_filings"
    if has_total:
        return "has_backlog_total"
    if has_keyword:
        return "has_backlog_keyword_only"
    if skipped_filings > 0:
        return "no_match_with_skips"
    return "no_backlog_signal"


def _apply_manual_status_override(corp_name: str, inferred_status: str) -> str:
    if corp_name in MANUAL_NO_BACKLOG_SIGNAL_COMPANIES and inferred_status == "no_match_with_skips":
        return "no_backlog_signal"
    return inferred_status


def _load_existing_results(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=RESULT_COLUMNS)
    df = pd.read_csv(path, dtype={"corp_code": "string", "stock_code": "string"})
    for column in RESULT_COLUMNS:
        if column not in df.columns:
            df[column] = None
    return _normalize_result_frame(df[RESULT_COLUMNS])


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    _normalize_result_frame(pd.DataFrame(rows, columns=RESULT_COLUMNS)).to_csv(path, index=False, encoding="utf-8-sig")


def _build_markdown_summary(df: pd.DataFrame, start_date: str, end_date: str) -> str:
    total_companies = len(df)
    keyword_count = int(df["has_backlog_keyword"].fillna(False).sum())
    total_count = int(df["has_backlog_total"].fillna(False).sum())
    no_filing_count = int((df["status"] == "no_regular_filings").sum())

    lines = ["# DART 상장사 수주잔고 분류", ""]
    lines.append(f"- 기준 기간: `{start_date}` ~ `{end_date}`")
    lines.append(f"- 전체 기업 수: `{total_companies}`")
    lines.append(f"- 수주잔고 관련 문구 존재: `{keyword_count}`")
    lines.append(f"- 수주잔고 총액 추출 가능: `{total_count}`")
    lines.append(f"- 정기보고서 없음: `{no_filing_count}`")
    lines.append("")

    lines.append("## 총액 추출 가능 기업")
    lines.append("")
    lines.append("| 기업명 | 종목코드 | 최신 기간 | 최신 수주잔고(억원) | 상태 |")
    lines.append("| --- | --- | --- | ---: | --- |")
    positive_df = df.loc[df["has_backlog_total"].fillna(False)].copy()
    positive_df = positive_df.sort_values(["corp_name", "stock_code"])
    for _, row in positive_df.iterrows():
        latest_total = row["latest_total_eok"]
        latest_display = "-" if pd.isna(latest_total) else _format_eok(float(latest_total))
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["corp_name"]),
                    str(row["stock_code"]),
                    str(row["latest_total_period"] or "-"),
                    latest_display,
                    str(row["status"]),
                ]
            )
            + " |"
        )

    lines.append("")
    lines.append("## 총액 미추출 기업")
    lines.append("")
    lines.append("| 기업명 | 종목코드 | 상태 |")
    lines.append("| --- | --- | --- |")
    negative_df = df.loc[~df["has_backlog_total"].fillna(False)].copy()
    negative_df = negative_df.sort_values(["corp_name", "stock_code"])
    for _, row in negative_df.iterrows():
        lines.append(
            "| "
            + " | ".join([str(row["corp_name"]), str(row["stock_code"]), str(row["status"])])
            + " |"
        )

    lines.append("")
    return "\n".join(lines)


def _format_eok(value: float) -> str:
    formatted = f"{value:,.2f}"
    return formatted.rstrip("0").rstrip(".")


def _normalize_result_frame(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    if "corp_code" in normalized.columns:
        normalized["corp_code"] = normalized["corp_code"].map(_normalize_corp_code)
    if "stock_code" in normalized.columns:
        normalized["stock_code"] = normalized["stock_code"].map(_normalize_stock_code)
    manual_mask = normalized["stock_code"].isin(MANUAL_NO_BACKLOG_STOCK_CODES)
    if manual_mask.any():
        normalized.loc[manual_mask, "has_backlog_keyword"] = False
        normalized.loc[manual_mask, "has_backlog_total"] = False
        normalized.loc[manual_mask, "latest_total_period"] = None
        normalized.loc[manual_mask, "latest_total_eok"] = None
        normalized.loc[manual_mask, "status"] = "manual_no_backlog"
        normalized.loc[manual_mask, "match_count"] = 0
        normalized.loc[manual_mask, "matched_filing_count"] = 0
        normalized.loc[manual_mask, "total_count"] = 0
    normalized = normalized.dropna(subset=["corp_code"])
    normalized = normalized.drop_duplicates(subset=["corp_code"], keep="last")
    return normalized[RESULT_COLUMNS]


def _normalize_corp_code(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    return digits.zfill(CORP_CODE_WIDTH)


def _normalize_stock_code(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return text
    return digits.zfill(STOCK_CODE_WIDTH)


if __name__ == "__main__":
    main()
