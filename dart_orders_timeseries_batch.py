from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from trade_tracker.config import get_settings
from trade_tracker.dart import DartClient, DartFiling
from dart_orders_timeseries import build_company_timeseries, _default_output_path


load_dotenv()


MANIFEST_COLUMNS = [
    "corp_code",
    "corp_name",
    "stock_code",
    "status",
    "message",
    "filings_found",
    "filings_scanned",
    "filings_skipped",
    "time_series_rows",
    "markdown_path",
    "cache_csv_path",
    "updated_at",
]

AGGREGATE_COLUMNS = [
    "corp_code",
    "corp_name",
    "stock_code",
    "filing_date",
    "report_name",
    "report_period",
    "amount_display",
    "change_display",
    "change_pct_display",
    "yoy_change_display",
    "yoy_change_pct_display",
    "amount_eok",
    "change_eok",
    "change_pct",
    "yoy_change_eok",
    "yoy_change_pct",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate order backlog time-series markdown files and a combined CSV for all has_backlog_total companies."
    )
    parser.add_argument(
        "--classification-csv",
        default="outputs/dart_listed_backlog_classification_latest1y_html_rerun.csv",
        help="Input classification CSV path",
    )
    parser.add_argument("--start-date", default="20220101", help="Start filing date, e.g. 20220101")
    parser.add_argument(
        "--end-date",
        default=date.today().strftime("%Y%m%d"),
        help="End filing date, e.g. 20260409",
    )
    parser.add_argument(
        "--document-source",
        choices=["html", "api"],
        default="html",
        help="How to load filing contents: direct DART HTML viewer or OpenDART document API",
    )
    parser.add_argument(
        "--html-request-interval",
        type=float,
        default=2.0,
        help="Minimum seconds between DART HTML requests when --document-source html is used",
    )
    parser.add_argument(
        "--cache-dir",
        default="outputs/.dart_cache",
        help="Cache directory for downloaded DART HTML pages",
    )
    parser.add_argument(
        "--timeseries-cache-dir",
        default="outputs/.timeseries_cache",
        help="Per-company time-series cache directory for fast resume",
    )
    parser.add_argument(
        "--filings-cache-dir",
        default="outputs/.filings_cache",
        help="Per-company filing-list cache directory for fast resume",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs/수주잔고",
        help="Directory for per-company markdown files",
    )
    parser.add_argument(
        "--output-csv",
        default="outputs/수주잔고/수주잔고_전체시계열.csv",
        help="Combined CSV output path",
    )
    parser.add_argument(
        "--manifest-csv",
        default="outputs/수주잔고/수주잔고_생성현황.csv",
        help="Manifest CSV for resume/skip tracking",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip companies already marked success in the manifest and with existing cache files",
    )
    parser.add_argument(
        "--finalize-only",
        action="store_true",
        help="Do not call DART again. Rebuild the combined CSV from cached per-company CSV files.",
    )
    parser.add_argument(
        "--aggregate-write-interval",
        type=int,
        default=50,
        help="Rebuild the combined CSV every N processed companies. Default: 50",
    )
    parser.add_argument("--limit", type=int, help="Optional limit for testing")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    classification_csv = Path(args.classification_csv)
    output_dir = Path(args.output_dir)
    output_csv = Path(args.output_csv)
    manifest_csv = Path(args.manifest_csv)
    timeseries_cache_dir = Path(args.timeseries_cache_dir)
    filings_cache_dir = Path(args.filings_cache_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    manifest_csv.parent.mkdir(parents=True, exist_ok=True)
    timeseries_cache_dir.mkdir(parents=True, exist_ok=True)
    filings_cache_dir.mkdir(parents=True, exist_ok=True)

    companies_df = _load_target_companies(classification_csv)
    if args.limit:
        companies_df = companies_df.head(args.limit).copy()

    manifest_df = _load_manifest(manifest_csv)

    if args.finalize_only:
        aggregate_df = _build_aggregate_from_cache(timeseries_cache_dir, manifest_df)
        aggregate_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        print(f"Combined CSV saved to: {output_csv}")
        print(f"Manifest CSV saved to: {manifest_csv}")
        print(f"Successful companies: {int((manifest_df['status'] == 'success').sum())}")
        return

    settings = get_settings()
    client = DartClient(
        api_key=settings.dart_api_key,
        document_source=args.document_source,
        html_request_interval=args.html_request_interval,
        cache_dir=args.cache_dir,
    )

    rows = manifest_df.to_dict(orient="records")
    manifest_index = {str(row["corp_code"]): idx for idx, row in enumerate(rows) if str(row.get("corp_code", ""))}
    targets = companies_df.to_dict(orient="records")
    dirty_aggregate = False
    processed_since_aggregate = 0

    for index, company_row in enumerate(targets, start=1):
        corp_code = str(company_row["corp_code"]).zfill(8)
        corp_name = str(company_row["corp_name"])
        stock_code = str(company_row["stock_code"]).zfill(6)
        markdown_path = output_dir / _default_output_path(corp_name, stock_code).name
        cache_csv_path = timeseries_cache_dir / f"{corp_code}.csv"
        filings_cache_path = filings_cache_dir / f"{corp_code}_{args.start_date}_{args.end_date}.json"

        existing_row = rows[manifest_index[corp_code]] if corp_code in manifest_index else None
        if args.resume and _can_skip_company(existing_row, markdown_path, cache_csv_path):
            print(f"{index:04d}/{len(targets):04d} {corp_name}({stock_code}) skipped=resume")
            continue

        try:
            filings = _load_or_fetch_filings(
                client=client,
                corp_code=corp_code,
                corp_name=corp_name,
                stock_code=stock_code,
                start_date=args.start_date,
                end_date=args.end_date,
                filings_cache_path=filings_cache_path,
            )
            result = build_company_timeseries(
                client,
                corp_code=corp_code,
                corp_name=corp_name,
                stock_code=stock_code,
                start_date=args.start_date,
                end_date=args.end_date,
                filings=filings,
            )
            series_df = result["series_df"].copy()
            series_df.insert(0, "stock_code", stock_code)
            series_df.insert(0, "corp_name", corp_name)
            series_df.insert(0, "corp_code", corp_code)
            series_df = series_df[AGGREGATE_COLUMNS]
            markdown_path.write_text(str(result["markdown"]), encoding="utf-8-sig")
            series_df.to_csv(cache_csv_path, index=False, encoding="utf-8-sig")
            manifest_row = {
                "corp_code": corp_code,
                "corp_name": corp_name,
                "stock_code": stock_code,
                "status": "success",
                "message": "",
                "filings_found": result["filings_found"],
                "filings_scanned": result["filings_scanned"],
                "filings_skipped": result["filings_skipped"],
                "time_series_rows": len(series_df),
                "markdown_path": str(markdown_path),
                "cache_csv_path": str(cache_csv_path),
                "updated_at": _now_text(),
            }
            print(
                f"{index:04d}/{len(targets):04d} {corp_name}({stock_code}) "
                f"rows={len(series_df)} filings={result['filings_found']} status=success"
            )
        except Exception as error:
            manifest_row = {
                "corp_code": corp_code,
                "corp_name": corp_name,
                "stock_code": stock_code,
                "status": "error",
                "message": str(error),
                "filings_found": 0,
                "filings_scanned": 0,
                "filings_skipped": 0,
                "time_series_rows": 0,
                "markdown_path": str(markdown_path),
                "cache_csv_path": str(cache_csv_path),
                "updated_at": _now_text(),
            }
            print(f"{index:04d}/{len(targets):04d} {corp_name}({stock_code}) status=error message={error}")

        if corp_code in manifest_index:
            rows[manifest_index[corp_code]] = manifest_row
        else:
            manifest_index[corp_code] = len(rows)
            rows.append(manifest_row)

        _write_manifest(manifest_csv, rows)
        dirty_aggregate = True
        processed_since_aggregate += 1
        if processed_since_aggregate >= max(args.aggregate_write_interval, 1):
            aggregate_df = _build_aggregate_from_cache(timeseries_cache_dir, pd.DataFrame(rows, columns=MANIFEST_COLUMNS))
            aggregate_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
            dirty_aggregate = False
            processed_since_aggregate = 0

    final_manifest_df = pd.DataFrame(rows, columns=MANIFEST_COLUMNS)
    _write_manifest(manifest_csv, rows)
    if dirty_aggregate or not output_csv.exists():
        aggregate_df = _build_aggregate_from_cache(timeseries_cache_dir, final_manifest_df)
        aggregate_df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    success_count = int((final_manifest_df["status"] == "success").sum()) if not final_manifest_df.empty else 0
    print(f"Combined CSV saved to: {output_csv}")
    print(f"Manifest CSV saved to: {manifest_csv}")
    print(f"Successful companies: {success_count}")


def _load_target_companies(classification_csv: Path) -> pd.DataFrame:
    if not classification_csv.exists():
        raise FileNotFoundError(f"Classification CSV not found: {classification_csv}")
    df = pd.read_csv(classification_csv, dtype={"corp_code": str, "stock_code": str})
    if "has_backlog_total" not in df.columns:
        raise ValueError("Classification CSV does not contain has_backlog_total column.")
    target_df = df.loc[df["has_backlog_total"].fillna(False)].copy()
    target_df["corp_code"] = target_df["corp_code"].astype(str).str.zfill(8)
    target_df["stock_code"] = target_df["stock_code"].astype(str).str.zfill(6)
    target_df = target_df[["corp_code", "corp_name", "stock_code"]].drop_duplicates()
    target_df = target_df.sort_values(["corp_name", "stock_code"]).reset_index(drop=True)
    return target_df


def _load_manifest(manifest_csv: Path) -> pd.DataFrame:
    if not manifest_csv.exists():
        return pd.DataFrame(columns=MANIFEST_COLUMNS)
    df = pd.read_csv(manifest_csv, dtype={"corp_code": str, "stock_code": str})
    for column in MANIFEST_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    df["corp_code"] = df["corp_code"].astype(str).str.zfill(8)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    return df[MANIFEST_COLUMNS]


def _can_skip_company(existing_row: dict[str, object] | None, markdown_path: Path, cache_csv_path: Path) -> bool:
    if not existing_row:
        return False
    return (
        str(existing_row.get("status", "")) == "success"
        and markdown_path.exists()
        and cache_csv_path.exists()
    )


def _load_or_fetch_filings(
    client: DartClient,
    corp_code: str,
    corp_name: str,
    stock_code: str,
    start_date: str,
    end_date: str,
    filings_cache_path: Path,
) -> list[DartFiling]:
    if filings_cache_path.exists():
        payload = json.loads(filings_cache_path.read_text(encoding="utf-8"))
        return [DartFiling(**item) for item in payload]

    filings = client.list_regular_filings(corp_code=corp_code, start_date=start_date, end_date=end_date)
    serialized = [
        {
            "corp_code": filing.corp_code,
            "corp_name": filing.corp_name,
            "stock_code": filing.stock_code,
            "report_code": filing.report_code,
            "report_label": filing.report_label,
            "report_name": filing.report_name,
            "receipt_no": filing.receipt_no,
            "filing_date": filing.filing_date,
        }
        for filing in filings
    ]
    filings_cache_path.write_text(json.dumps(serialized, ensure_ascii=False, indent=2), encoding="utf-8")
    return filings


def _build_aggregate_from_cache(timeseries_cache_dir: Path, manifest_df: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if manifest_df.empty:
        return pd.DataFrame(columns=AGGREGATE_COLUMNS)

    success_df = manifest_df.loc[manifest_df["status"] == "success"].copy()
    for _, row in success_df.iterrows():
        cache_csv_path = Path(str(row["cache_csv_path"]))
        if not cache_csv_path.exists():
            fallback_path = timeseries_cache_dir / f"{str(row['corp_code']).zfill(8)}.csv"
            cache_csv_path = fallback_path
        if not cache_csv_path.exists():
            continue
        frame = pd.read_csv(cache_csv_path, dtype={"corp_code": str, "stock_code": str})
        for column in AGGREGATE_COLUMNS:
            if column not in frame.columns:
                frame[column] = None
        frames.append(frame[AGGREGATE_COLUMNS])

    if not frames:
        return pd.DataFrame(columns=AGGREGATE_COLUMNS)
    aggregate_df = pd.concat(frames, ignore_index=True)
    aggregate_df = aggregate_df.sort_values(["corp_name", "report_period", "filing_date"]).reset_index(drop=True)
    return aggregate_df


def _write_manifest(manifest_csv: Path, rows: list[dict[str, object]]) -> None:
    manifest_df = pd.DataFrame(rows, columns=MANIFEST_COLUMNS)
    if not manifest_df.empty:
        manifest_df = manifest_df.sort_values(["status", "corp_name", "stock_code"]).reset_index(drop=True)
    manifest_df.to_csv(manifest_csv, index=False, encoding="utf-8-sig")


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    main()
