from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from trade_tracker.config import get_settings
from trade_tracker.dart import (
    DartCompany,
    DartClient,
    batch_totals_to_markdown,
    build_total_summary,
    extract_order_backlog_matches,
)


load_dotenv()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a company-by-period markdown matrix of DART order backlog totals."
    )
    parser.add_argument(
        "--companies",
        nargs="+",
        required=True,
        help="Company names or stock codes",
    )
    parser.add_argument("--start-date", required=True, help="Start filing date, e.g. 20240101")
    parser.add_argument("--end-date", required=True, help="End filing date, e.g. 20241231")
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
        "--output",
        default="outputs/order_backlog_batch.md",
        help="Markdown output path",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    settings = get_settings()
    client = DartClient(
        api_key=settings.dart_api_key,
        document_source=args.document_source,
        html_request_interval=args.html_request_interval,
        cache_dir=args.cache_dir,
    )

    results: list[tuple[DartCompany, pd.DataFrame]] = []
    for query in args.companies:
        try:
            company = client.find_company(query)
            filings = client.list_regular_filings(
                corp_code=company.corp_code,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            matches = []
            for filing in filings:
                try:
                    files = client.download_original_document(filing.receipt_no)
                except Exception as error:
                    print(f"{company.corp_name}: skipped filing {filing.receipt_no} ({error})")
                    continue
                matches.extend(extract_order_backlog_matches(filing, files))

            total_df = build_total_summary(pd.DataFrame([match.__dict__ for match in matches]), stock_code=company.stock_code)
            results.append((company, total_df))
            print(f"{company.corp_name}: filings={len(filings)}, matches={len(matches)}, totals={len(total_df)}")
        except Exception as error:
            placeholder = DartCompany(corp_code="-", corp_name=query, stock_code="-", modify_date="")
            results.append((placeholder, pd.DataFrame()))
            print(f"{query}: error={error}")

    markdown = batch_totals_to_markdown(results)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    print(f"Markdown saved to: {output_path}")


if __name__ == "__main__":
    main()
