from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from trade_tracker.config import get_settings
from trade_tracker.dart import DartClient, extract_order_backlog_matches, matches_to_markdown


load_dotenv()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract quarterly/semiannual/annual order backlog amounts from DART filings."
    )
    parser.add_argument("--company", required=True, help="Company name or stock code")
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
        help="Markdown output path. Defaults to outputs/<company>_order_backlog.md",
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
    company = client.find_company(args.company)
    filings = client.list_regular_filings(
        corp_code=company.corp_code,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    if not filings:
        raise SystemExit("No regular DART filings found in the requested date range.")

    all_matches = []
    for filing in filings:
        try:
            files = client.download_original_document(filing.receipt_no)
        except Exception as error:
            print(f"Skipped filing {filing.receipt_no}: {error}")
            continue
        all_matches.extend(extract_order_backlog_matches(filing, files))

    markdown = matches_to_markdown(company, all_matches)
    output_path = Path(args.output) if args.output else _default_output_path(company.corp_name)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")

    print(f"Company: {company.corp_name} ({company.corp_code})")
    print(f"Filings scanned: {len(filings)}")
    print(f"Matches found: {len(all_matches)}")
    print(f"Markdown saved to: {output_path}")


def _default_output_path(company_name: str) -> Path:
    sanitized = "".join(char if char.isalnum() else "_" for char in company_name).strip("_")
    return Path("outputs") / f"{sanitized}_order_backlog.md"


if __name__ == "__main__":
    main()
