from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from trade_tracker.config import get_settings
from trade_tracker.dart import DartClient, DartFiling, build_total_summary, extract_order_backlog_matches


load_dotenv()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a quarterly order backlog time series from DART regular filings."
    )
    parser.add_argument("--company", required=True, help="Company name or stock code")
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
        default=3.0,
        help="Minimum seconds between DART HTML requests when --document-source html is used",
    )
    parser.add_argument(
        "--cache-dir",
        help="Optional cache directory for downloaded DART HTML pages",
    )
    parser.add_argument(
        "--output",
        help="Markdown output path. Defaults to outputs/수주잔고/<company>_수주잔고(<stock_code>).md",
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
    result = build_company_timeseries(client, company.corp_code, company.corp_name, company.stock_code, args.start_date, args.end_date)
    series_df = result["series_df"]
    markdown = result["markdown"]

    output_path = Path(args.output) if args.output else _default_output_path(company.corp_name, company.stock_code)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8-sig")

    print(f"Company: {company.corp_name} ({company.stock_code or '-'})")
    print(f"Filings found: {result['filings_found']}")
    print(f"Filings scanned: {result['filings_scanned']}")
    print(f"Filings skipped: {result['filings_skipped']}")
    print(f"Time-series rows: {len(series_df)}")
    print(f"Markdown saved to: {output_path}")


def build_company_timeseries(
    client: DartClient,
    corp_code: str,
    corp_name: str,
    stock_code: str | None,
    start_date: str,
    end_date: str,
    filings: list[DartFiling] | None = None,
) -> dict[str, object]:
    if filings is None:
        filings = client.list_regular_filings(
            corp_code=corp_code,
            start_date=start_date,
            end_date=end_date,
        )
    if not filings:
        raise ValueError("No regular DART filings found in the requested date range.")

    all_matches = []
    scanned_filings = 0
    skipped_filings = 0
    for filing in filings:
        try:
            files = client.download_original_document(filing.receipt_no)
        except Exception:
            skipped_filings += 1
            continue
        scanned_filings += 1
        all_matches.extend(extract_order_backlog_matches(filing, files))

    match_df = pd.DataFrame([match.__dict__ for match in all_matches])
    if stock_code == "011930":
        segmented = _build_manual_segmented_series(match_df)
        markdown = _segmented_timeseries_to_markdown(
            corp_name,
            stock_code or "-",
            segmented,
            start_date,
            end_date,
        )
        primary_series = segmented["클린환경"]["series_df"]
        aggregate_frames = []
        for segment_name in ["클린환경", "재생에너지"]:
            segment_series = segmented[segment_name]["series_df"].copy()
            if segment_series.empty:
                continue
            segment_series.insert(0, "stock_code", stock_code or "-")
            segment_series.insert(0, "corp_name", f"{corp_name}({segment_name})")
            segment_series.insert(0, "corp_code", f"{corp_code}_{'clean' if segment_name == '클린환경' else 'renew'}")
            aggregate_frames.append(segment_series)
        aggregate_df = pd.concat(aggregate_frames, ignore_index=True) if aggregate_frames else pd.DataFrame()
        notes = segmented["클린환경"]["notes"] + segmented["재생에너지"]["notes"]
        return {
            "corp_code": corp_code,
            "corp_name": corp_name,
            "stock_code": stock_code or "-",
            "start_date": start_date,
            "end_date": end_date,
            "filings_found": len(filings),
            "filings_scanned": scanned_filings,
            "filings_skipped": skipped_filings,
            "series_df": primary_series,
            "aggregate_df": aggregate_df,
            "notes": notes,
            "markdown": markdown,
        }
    match_df, filter_notes = _apply_manual_match_filters(match_df, corp_name, stock_code)
    total_df, summary_notes = _build_timeseries_total_summary(match_df)
    total_df, override_notes = _apply_manual_timeseries_overrides(total_df, corp_name, stock_code)
    summary_notes = filter_notes + summary_notes
    summary_notes.extend(override_notes)
    if total_df.empty:
        raise ValueError("No order backlog totals could be extracted from the requested filings.")
    series_df, notes = _build_series_frame(total_df)
    notes = summary_notes + notes
    markdown = _timeseries_to_markdown(
        corp_name,
        stock_code or "-",
        series_df,
        notes,
        start_date,
        end_date,
    )
    return {
        "corp_code": corp_code,
        "corp_name": corp_name,
        "stock_code": stock_code or "-",
        "start_date": start_date,
        "end_date": end_date,
        "filings_found": len(filings),
        "filings_scanned": scanned_filings,
        "filings_skipped": skipped_filings,
        "series_df": series_df,
        "aggregate_df": pd.DataFrame(),
        "notes": notes,
        "markdown": markdown,
    }


def _build_timeseries_total_summary(match_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    total_df = build_total_summary(match_df)
    if total_df.empty:
        return total_df, []
    return total_df, []

    candidate_df = _build_total_candidates(match_df)
    if candidate_df.empty:
        return total_df, []

    notes: list[str] = []
    corrected_rows = []
    for _, row in total_df.iterrows():
        corrected_row = row.copy()
        filing_candidates = candidate_df.loc[
            (candidate_df["filing_date"] == row["filing_date"])
            & (candidate_df["report_name"] == row["report_name"])
        ].copy()
        if not filing_candidates.empty:
            filing_candidates = filing_candidates.sort_values("amount_krw", ascending=False)
            max_candidate = filing_candidates.iloc[0]
            selected_amount = float(row["amount_krw"])
            max_amount = float(max_candidate["amount_krw"])
            if selected_amount > 0 and max_amount >= selected_amount * 20:
                corrected_row["amount_krw"] = int(max_amount)
                corrected_row["amount_eok"] = max_amount / 100_000_000
                corrected_row["amount_display"] = _format_number(corrected_row["amount_eok"])
                notes.append(
                    f"`{row['report_period']}` (`{row['filing_date']} {row['report_name']}`)은 "
                    f"동일 보고서 내 총액 후보 간 배율 차이가 커서 더 큰 후보값으로 교정했습니다."
                )
        corrected_rows.append(corrected_row)

    corrected_df = pd.DataFrame(corrected_rows)
    corrected_df = corrected_df[["filing_date", "report_name", "report_period", "amount_display", "amount_krw", "amount_eok"]]
    return corrected_df, notes


def _apply_manual_match_filters(
    match_df: pd.DataFrame,
    corp_name: str,
    stock_code: str | None,
) -> tuple[pd.DataFrame, list[str]]:
    if match_df.empty or stock_code != "030530":
        return match_df, []

    company_token = "\uc6d0\uc775\ud640\ub529\uc2a4"
    filtered_df = match_df.loc[
        match_df["matched_text"].fillna("").str.contains(company_token, regex=False)
    ].copy()
    removed_count = len(match_df) - len(filtered_df)
    if removed_count <= 0:
        return filtered_df, []

    note = (
        f"`{corp_name}` 은 자회사 수주잔고 혼입 방지를 위해 "
        f"`matched_text` 에 `{corp_name}` 가 직접 포함된 후보만 인정하도록 수동 필터를 적용했습니다."
    )
    return filtered_df, [note]


def _apply_manual_timeseries_overrides(
    total_df: pd.DataFrame,
    corp_name: str,
    stock_code: str | None,
) -> tuple[pd.DataFrame, list[str]]:
    if total_df.empty or stock_code != "044180":
        return total_df, []

    overrides = {
        ("2025.03", "분기보고서 (2025.03)"): 434.0,
        ("2025.06", "반기보고서 (2025.06)"): 462.0,
        ("2025.09", "[기재정정]분기보고서 (2025.09)"): 426.0,
    }

    adjusted_df = total_df.copy()
    notes: list[str] = []
    for (report_period, report_name), amount_eok in overrides.items():
        mask = (adjusted_df["report_period"] == report_period) & (adjusted_df["report_name"] == report_name)
        if not mask.any():
            continue
        amount_krw = int(amount_eok * 100_000_000)
        adjusted_df.loc[mask, "amount_krw"] = amount_krw
        adjusted_df.loc[mask, "amount_eok"] = amount_eok
        adjusted_df.loc[mask, "amount_display"] = _format_number(amount_eok)
        notes.append(
            f"`{corp_name}` `{report_name}` (`{report_period}`) 은 보고서 단위 오류 예외로 "
            f"수주잔고를 `{_format_number(amount_eok)}`억원으로 고정했습니다."
        )

    return adjusted_df, notes


def _build_manual_segmented_series(match_df: pd.DataFrame) -> dict[str, dict[str, object]]:
    segment_tokens = {
        "클린환경": "\ud074\ub9b0\ub8f8 \ubc0f \uacf5\uc870\uc2dc\uc2a4\ud15c \uc81c\uc870, \uc124\uce58\uacf5\uc0ac \uc678",
        "재생에너지": "\ud0dc\uc591\uad11 \ubaa8\ub4c8 \ub4f1",
    }
    source_priority = {"section_table": 0, "xml_table": 0, "table": 1, "snippet": 2, "generic": 3}
    results: dict[str, dict[str, object]] = {}

    for segment_name, token in segment_tokens.items():
        segment_df = match_df.loc[match_df["matched_text"].fillna("").str.contains(token, regex=False)].copy()
        if segment_df.empty:
            results[segment_name] = {"series_df": pd.DataFrame(), "notes": [f"`{segment_name}` 후보를 찾지 못했습니다."]}
            continue

        segment_df["amount_krw"] = pd.to_numeric(segment_df["amount_krw"], errors="coerce")
        segment_df = segment_df.dropna(subset=["amount_krw"]).copy()
        segment_df["source_priority"] = segment_df["source_kind"].map(lambda value: source_priority.get(value, 9))
        segment_df["report_period"] = segment_df["report_name"].map(_extract_report_period)
        segment_df = segment_df.sort_values(
            ["filing_date", "report_name", "amount_krw", "source_priority"],
            ascending=[True, True, False, True],
        )
        segment_df = segment_df.drop_duplicates(subset=["filing_date", "report_name"], keep="first")
        segment_df["amount_eok"] = segment_df["amount_krw"] / 100_000_000
        segment_df["amount_display"] = segment_df["amount_eok"].map(_format_number)
        total_df = segment_df[["filing_date", "report_name", "report_period", "amount_display", "amount_krw", "amount_eok"]].copy()
        series_df, series_notes = _build_series_frame(total_df)
        notes = [f"`{segment_name}` 은 `{token}` 라벨이 포함된 후보만 사용했습니다."]
        if segment_name == "클린환경":
            notes.append("같은 보고서에 클린환경 후보가 여러 개면 더 큰 값을 대표값으로 사용했습니다.")
        notes.extend(series_notes)
        results[segment_name] = {"series_df": series_df, "notes": notes}

    return results


def _build_total_candidates(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = ["filing_date", "report_name", "report_period", "amount_krw", "amount_eok", "matched_text", "source_kind"]
    if df.empty or "matched_text" not in df.columns:
        return pd.DataFrame(columns=expected_columns)

    candidate_df = df.copy()
    total_mask = (
        candidate_df["matched_text"].str.contains(
            r"(?:합\s*계|총\s*계|연결합계|기말수주잔고|수주잔고\s*총액|계약잔액)",
            na=False,
            regex=True,
        )
        | candidate_df["matched_text"].str.contains(r"\|\s*계\s*\|", na=False, regex=True)
    )
    candidate_df = candidate_df.loc[total_mask].copy()
    candidate_df = candidate_df.dropna(subset=["amount_krw"])
    if candidate_df.empty:
        return pd.DataFrame(columns=expected_columns)

    candidate_df["report_period"] = candidate_df["report_name"].map(_extract_report_period)
    candidate_df["amount_eok"] = candidate_df["amount_krw"].map(lambda value: value / 100_000_000 if pd.notna(value) else None)
    return candidate_df[expected_columns]


def _build_series_frame(total_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    candidate_df = total_df.copy()
    candidate_df = candidate_df.sort_values(["report_period", "filing_date", "report_name"]).reset_index(drop=True)
    series_df, notes = _select_period_candidates(candidate_df)
    series_df, normalization_notes = _normalize_outliers(series_df)
    notes.extend(normalization_notes)
    series_df["amount_eok"] = series_df["amount_krw"].map(lambda value: value / 100_000_000 if pd.notna(value) else None)
    series_df["change_eok"] = series_df["amount_eok"].diff()
    series_df["change_pct"] = series_df["amount_eok"].pct_change() * 100
    series_df["yoy_change_eok"], series_df["yoy_change_pct"] = _build_yoy_changes(series_df)
    series_df["amount_display"] = series_df["amount_eok"].map(_format_number)
    series_df["change_display"] = series_df["change_eok"].map(_format_delta)
    series_df["change_pct_display"] = series_df["change_pct"].map(_format_pct)
    series_df["yoy_change_display"] = series_df["yoy_change_eok"].map(_format_delta)
    series_df["yoy_change_pct_display"] = series_df["yoy_change_pct"].map(_format_pct)
    return (
        series_df[
            [
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
        ],
        notes,
    )


def _select_period_candidates(candidate_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    grouped = {
        period: group.sort_values(["filing_date", "report_name"]).reset_index(drop=True)
        for period, group in candidate_df.groupby("report_period", sort=True)
    }
    ordered_periods = sorted(grouped.keys())
    selected_rows: list[pd.Series] = [group.iloc[-1].copy() for _, group in sorted(grouped.items())]
    notes: list[str] = []

    for index, period in enumerate(ordered_periods):
        group = grouped[period]
        if len(group) <= 1:
            continue

        current = selected_rows[index]
        replacement, reason = _choose_better_period_candidate(selected_rows, index, group)
        if replacement is not None and replacement["filing_date"] != current["filing_date"]:
            selected_rows[index] = replacement.copy()
            notes.append(
                f"`{period}` 기간은 중복 공시 {len(group)}건 중 "
                f"`{current['filing_date']} {current['report_name']}` 대신 "
                f"`{replacement['filing_date']} {replacement['report_name']}` 를 사용했습니다. 이유: {reason}."
            )
        else:
            notes.append(
                f"`{period}` 기간은 중복 공시 {len(group)}건이 있어 "
                f"최신 공시인 `{current['filing_date']} {current['report_name']}` 를 사용했습니다."
            )

    result_df = pd.DataFrame(selected_rows)
    result_df = result_df.sort_values("report_period").reset_index(drop=True)
    return result_df, notes


def _choose_better_period_candidate(
    selected_rows: list[pd.Series],
    index: int,
    group: pd.DataFrame,
) -> tuple[pd.Series | None, str]:
    current = selected_rows[index]
    previous_amount = _neighbor_amount(selected_rows, index - 1)
    next_amount = _neighbor_amount(selected_rows, index + 1)
    if previous_amount is None or next_amount is None:
        return None, ""

    current_amount = float(current["amount_krw"])
    if not _is_clear_outlier(current_amount, previous_amount, next_amount):
        return None, ""

    target_amount = (previous_amount + next_amount) / 2
    best_row = min(group.itertuples(index=False), key=lambda row: abs(float(row.amount_krw) - target_amount))
    if float(best_row.amount_krw) == current_amount:
        return None, ""
    return pd.Series(best_row._asdict()), "주변 분기 대비 급격한 이상치 완화"


def _normalize_outliers(series_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    normalized_df = series_df.copy()
    normalized_df["amount_krw"] = pd.to_numeric(normalized_df["amount_krw"], errors="coerce").astype(float)
    notes: list[str] = []
    normalized_df, pair_notes = _normalize_outlier_pairs(normalized_df)
    notes.extend(pair_notes)

    for index in range(len(normalized_df)):
        current_amount = _neighbor_amount_from_frame(normalized_df, index)
        previous_amount = _neighbor_amount_from_frame(normalized_df, index - 1)
        next_amount = _neighbor_amount_from_frame(normalized_df, index + 1)
        if current_amount is None or previous_amount is None or next_amount is None:
            continue
        if not _is_clear_outlier(current_amount, previous_amount, next_amount):
            continue

        replacement, factor = _rescaled_amount(current_amount, previous_amount, next_amount)
        period = normalized_df.at[index, "report_period"]
        filing_label = f"{normalized_df.at[index, 'filing_date']} {normalized_df.at[index, 'report_name']}"
        if replacement is not None and factor is not None:
            normalized_df.at[index, "amount_krw"] = replacement
            notes.append(
                f"`{period}` (`{filing_label}`) 값은 주변 분기와 비교해 단위 오인 가능성이 커서 "
                f"`x{factor:g}` 보정을 적용했습니다."
            )
            continue

        normalized_df.at[index, "amount_krw"] = pd.NA
        notes.append(
            f"`{period}` (`{filing_label}`) 값은 주변 분기 대비 이상치로 판단되어 자동 제외했습니다."
        )

    return normalized_df, notes


def _normalize_outlier_pairs(series_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    normalized_df = series_df.copy()
    notes: list[str] = []

    for index in range(1, len(normalized_df) - 2):
        previous_amount = _neighbor_amount_from_frame(normalized_df, index - 1)
        current_amount = _neighbor_amount_from_frame(normalized_df, index)
        next_amount = _neighbor_amount_from_frame(normalized_df, index + 1)
        following_amount = _neighbor_amount_from_frame(normalized_df, index + 2)
        if None in (previous_amount, current_amount, next_amount, following_amount):
            continue

        lower_bound = min(previous_amount, following_amount)
        upper_bound = max(previous_amount, following_amount)
        if lower_bound <= 0:
            continue
        if not (
            current_amount < lower_bound * 0.2
            and next_amount < lower_bound * 0.2
            or current_amount > upper_bound * 5
            and next_amount > upper_bound * 5
        ):
            continue

        factor = _best_pair_scale_factor(previous_amount, current_amount, next_amount, following_amount)
        if factor is None:
            continue

        normalized_df.at[index, "amount_krw"] = current_amount * factor
        normalized_df.at[index + 1, "amount_krw"] = next_amount * factor
        first_label = f"{normalized_df.at[index, 'report_period']} ({normalized_df.at[index, 'filing_date']} {normalized_df.at[index, 'report_name']})"
        second_label = f"{normalized_df.at[index + 1, 'report_period']} ({normalized_df.at[index + 1, 'filing_date']} {normalized_df.at[index + 1, 'report_name']})"
        notes.append(
            f"`{first_label}` 와 `{second_label}` 값은 연속 분기 단위 오인 가능성이 커서 `x{factor:g}` 보정을 적용했습니다."
        )

    return normalized_df, notes


def _build_yoy_changes(series_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    amount_by_period = {
        str(row["report_period"]): row["amount_eok"]
        for _, row in series_df.iterrows()
        if pd.notna(row["amount_eok"])
    }
    yoy_change_values: list[float | None] = []
    yoy_change_pct_values: list[float | None] = []

    for _, row in series_df.iterrows():
        current_period = str(row["report_period"])
        current_amount = row["amount_eok"]
        previous_year_period = _previous_year_period(current_period)
        if previous_year_period is None or pd.isna(current_amount):
            yoy_change_values.append(None)
            yoy_change_pct_values.append(None)
            continue

        previous_amount = amount_by_period.get(previous_year_period)
        if previous_amount is None or pd.isna(previous_amount):
            yoy_change_values.append(None)
            yoy_change_pct_values.append(None)
            continue

        yoy_change = float(current_amount) - float(previous_amount)
        yoy_change_values.append(yoy_change)
        if float(previous_amount) == 0:
            yoy_change_pct_values.append(None)
        else:
            yoy_change_pct_values.append((yoy_change / float(previous_amount)) * 100)

    return pd.Series(yoy_change_values), pd.Series(yoy_change_pct_values)


def _neighbor_amount(selected_rows: list[pd.Series], index: int) -> float | None:
    if index < 0 or index >= len(selected_rows):
        return None
    value = selected_rows[index].get("amount_krw")
    if pd.isna(value):
        return None
    return float(value)


def _neighbor_amount_from_frame(frame: pd.DataFrame, index: int) -> float | None:
    if index < 0 or index >= len(frame):
        return None
    value = frame.at[index, "amount_krw"]
    if pd.isna(value):
        return None
    return float(value)


def _rescaled_amount(current: float, previous: float, following: float) -> tuple[float | None, float | None]:
    target_amount = (previous + following) / 2
    candidate_factors = [10, 100, 1000, 10000, 0.1, 0.01, 0.001, 0.0001]
    current_distance = abs(current - target_amount)
    best_amount = None
    best_factor = None
    best_distance = current_distance

    for factor in candidate_factors:
        candidate_amount = current * factor
        candidate_distance = abs(candidate_amount - target_amount)
        if candidate_distance >= best_distance:
            continue
        if _is_clear_outlier(candidate_amount, previous, following):
            continue
        best_amount = candidate_amount
        best_factor = factor
        best_distance = candidate_distance

    if best_amount is None:
        return None, None
    if current_distance <= 0:
        return None, None
    if best_distance > current_distance * 0.2:
        return None, None
    return best_amount, best_factor


def _best_pair_scale_factor(
    previous: float,
    current: float,
    following: float,
    next_anchor: float,
) -> float | None:
    candidate_factors = [10, 100, 1000, 10000, 0.1, 0.01, 0.001, 0.0001]
    target_first = (previous + next_anchor) / 2
    target_second = (previous + next_anchor) / 2
    current_distance = abs(current - target_first) + abs(following - target_second)
    best_factor = None
    best_distance = current_distance

    for factor in candidate_factors:
        scaled_first = current * factor
        scaled_second = following * factor
        candidate_distance = abs(scaled_first - target_first) + abs(scaled_second - target_second)
        if candidate_distance >= best_distance:
            continue
        if _is_clear_outlier(scaled_first, previous, next_anchor):
            continue
        if _is_clear_outlier(scaled_second, previous, next_anchor):
            continue
        best_factor = factor
        best_distance = candidate_distance

    if best_factor is None:
        return None
    if best_distance > current_distance * 0.2:
        return None
    return best_factor


def _is_clear_outlier(current: float, previous: float, following: float) -> bool:
    lower_bound = min(previous, following)
    upper_bound = max(previous, following)
    if lower_bound <= 0:
        return False
    return current < lower_bound * 0.2 or current > upper_bound * 5


def _timeseries_to_markdown(
    company_name: str,
    stock_code: str,
    series_df: pd.DataFrame,
    notes: list[str],
    start_date: str,
    end_date: str,
) -> str:
    latest_row = series_df.dropna(subset=["amount_eok"]).iloc[-1]
    lines = [f"# {company_name} 분기별 수주잔고 변화", ""]
    lines.append(f"- 종목코드: `{stock_code}`")
    lines.append(f"- 기준 기간: `{start_date}` ~ `{end_date}`")
    lines.append(f"- 추출 건수: `{len(series_df)}`")
    lines.append(f"- 최신 기간: `{latest_row['report_period']}`")
    lines.append(f"- 최신 수주잔고: `{latest_row['amount_display']}` 억원")
    lines.append("")
    if notes:
        lines.append("## 선택 메모")
        lines.append("")
        for note in notes:
            lines.append(f"- {note}")
        lines.append("")
    lines.append("## 시계열 표")
    lines.append("")
    lines.append("| 공시일 | 보고서 | 기간 | 수주잔고(억원) | 전기 대비 증감(억원) | 전기 대비 증감률 | YoY 증감(억원) | YoY 증감률 |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |")
    for _, row in series_df.iterrows():
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["filing_date"]),
                    str(row["report_name"]),
                    str(row["report_period"]),
                    str(row["amount_display"]),
                    str(row["change_display"]),
                    str(row["change_pct_display"]),
                    str(row["yoy_change_display"]),
                    str(row["yoy_change_pct_display"]),
                ]
            )
            + " |"
        )
    lines.append("")
    lines.append("## 해석")
    lines.append("")
    lines.append("- 값은 각 정기보고서에서 추출된 총 수주잔고 후보를 `억원` 기준으로 환산한 값입니다.")
    lines.append("- 전기 대비 증감은 직전 유효 분기 대비 변화입니다.")
    lines.append("- YoY 증감은 전년 동기 대비 변화입니다.")
    lines.append("- 같은 기간 정정공시가 여러 건이면 최신 공시를 기본으로 사용하되, 주변 분기 대비 명백한 이상치는 보정하거나 제외합니다.")
    lines.append("")
    return "\n".join(lines)


def _segmented_timeseries_to_markdown(
    company_name: str,
    stock_code: str,
    segmented: dict[str, dict[str, object]],
    start_date: str,
    end_date: str,
) -> str:
    lines = [f"# {company_name} 사업부문별 분기 수주잔고 변화", ""]
    lines.append(f"- 종목코드: `{stock_code}`")
    lines.append(f"- 기준 기간: `{start_date}` ~ `{end_date}`")
    lines.append("")

    for segment_name in ["클린환경", "재생에너지"]:
        segment = segmented[segment_name]
        series_df = segment["series_df"]
        notes = segment["notes"]
        lines.append(f"## {segment_name}")
        lines.append("")
        if series_df.empty:
            lines.append("- 추출 결과가 없습니다.")
            lines.append("")
            continue
        latest_row = series_df.dropna(subset=["amount_eok"]).iloc[-1]
        lines.append(f"- 추출 건수: `{len(series_df)}`")
        lines.append(f"- 최신 기간: `{latest_row['report_period']}`")
        lines.append(f"- 최신 수주잔고: `{latest_row['amount_display']}` 억원")
        lines.append("")
        if notes:
            lines.append("### 선택 메모")
            lines.append("")
            for note in notes:
                lines.append(f"- {note}")
            lines.append("")
        lines.append("### 시계열 표")
        lines.append("")
        lines.append("| 공시일 | 보고서 | 기간 | 수주잔고(억원) | 전기 대비 증감(억원) | 전기 대비 증감률 | YoY 증감(억원) | YoY 증감률 |")
        lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |")
        for _, row in series_df.iterrows():
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row["filing_date"]),
                        str(row["report_name"]),
                        str(row["report_period"]),
                        str(row["amount_display"]),
                        str(row["change_display"]),
                        str(row["change_pct_display"]),
                        str(row["yoy_change_display"]),
                        str(row["yoy_change_pct_display"]),
                    ]
                )
                + " |"
            )
        lines.append("")

    lines.append("## 해석")
    lines.append("")
    lines.append("- 클린환경과 재생에너지 사업부문은 각각 별도 시계열로 분리했습니다.")
    lines.append("- 같은 보고서에서 동일 사업부문 후보가 여러 개면 대표값 1개만 선택했습니다.")
    lines.append("- 값은 각 보고서 수주상황 표에서 추출한 사업부문별 수주잔고를 `억원` 기준으로 환산한 값입니다.")
    lines.append("")
    return "\n".join(lines)


def _format_number(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.2f}".rstrip("0").rstrip(".")


def _format_delta(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:,.2f}".rstrip("0").rstrip(".")


def _format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:,.2f}%".rstrip("0").rstrip(".")


def _default_output_path(company_name: str, stock_code: str | None) -> Path:
    sanitized_name = "".join(char if char.isalnum() else "_" for char in company_name).strip("_")
    sanitized_code = "".join(char for char in (stock_code or "-") if char.isalnum()) or "-"
    file_name = f"{sanitized_name}_수주잔고({sanitized_code}).md"
    return Path("outputs") / "수주잔고" / file_name


def _extract_report_period(report_name: str) -> str:
    if "(" in report_name and ")" in report_name:
        return report_name.split("(", 1)[1].split(")", 1)[0].strip()
    return report_name


def _previous_year_period(report_period: str) -> str | None:
    if "." not in report_period:
        return None
    year_text, month_text = report_period.split(".", 1)
    if not year_text.isdigit():
        return None
    return f"{int(year_text) - 1}.{month_text}"


if __name__ == "__main__":
    main()
