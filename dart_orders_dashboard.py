from __future__ import annotations

import argparse
import html
from pathlib import Path

import pandas as pd


QUARTER_SPECS = [
    ("2025.12", "사업보고서", "2025년 4분기"),
    ("2025.09", "분기보고서", "2025년 3분기"),
    ("2025.06", "반기보고서", "2025년 2분기"),
    ("2025.03", "분기보고서", "2025년 1분기"),
]
GROWTH_STREAK_THRESHOLD_PCT = 20.0
GROWTH_STREAK_MIN_QUARTERS = 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build an order backlog dashboard markdown from the combined timeseries CSV.")
    parser.add_argument(
        "--input-csv",
        default="outputs/수주잔고/수주잔고_전체시계열.csv",
        help="Combined timeseries CSV path",
    )
    parser.add_argument(
        "--output-md",
        default="outputs/수주잔고/수주잔고_대시보드.md",
        help="Dashboard markdown output path",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    input_csv = Path(args.input_csv)
    output_md = Path(args.output_md)

    df = pd.read_csv(input_csv, dtype={"corp_code": str, "stock_code": str}, encoding="utf-8-sig")
    for column in ["amount_eok", "change_eok", "change_pct", "yoy_change_eok", "yoy_change_pct"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    lines = [
        "# 수주잔고 대시보드",
        "",
        f"- 기준 데이터: `{input_csv.as_posix()}`",
        "- 기준 범위: `2025년 1분기 ~ 2025년 4분기`",
        "- 기업명 링크: 각 기업별 수주잔고 MD 문서로 연결",
        "- 주의: 자동 추출 결과이므로 극단값은 개별 기업 문서를 함께 확인하는 편이 안전합니다.",
        "",
    ]

    for index, (period, report_kind, label) in enumerate(QUARTER_SPECS):
        lines.extend(_build_quarter_section(df, period, report_kind, label, open_by_default=index == 0))

    lines.extend(
        [
            "## 메모",
            "",
            "- 순위는 자동 추출된 시계열 기준으로 계산합니다.",
            "- 증감률은 분모가 매우 작은 경우 과도하게 커질 수 있습니다.",
            "- 실제 해석 전에는 개별 기업 문서와 원문 공시를 함께 확인하는 편이 안전합니다.",
            "",
        ]
    )

    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text("\n".join(lines), encoding="utf-8-sig")
    print(f"Dashboard saved to: {output_md}")


def _build_quarter_section(
    df: pd.DataFrame,
    period: str,
    report_kind: str,
    label: str,
    open_by_default: bool = False,
) -> list[str]:
    quarter_df = df[
        df["report_name"].astype(str).str.contains(report_kind, na=False, regex=False)
        & df["report_name"].astype(str).str.contains(period, na=False, regex=False)
    ].copy()
    quarter_df = quarter_df.sort_values(["corp_name", "stock_code", "filing_date"]).drop_duplicates(["corp_code"], keep="last")

    details_tag = "<details open>" if open_by_default else "<details>"
    lines = [details_tag, f"<summary><strong>{label}</strong></summary>", ""]
    if quarter_df.empty:
        lines.extend(["<p>데이터 없음</p>", "", "</details>", ""])
        return lines

    lines.extend(
        _build_table(
            f"{label} 전기 대비 증감률 Top 15",
            quarter_df.dropna(subset=["change_pct"]).sort_values(["change_pct", "amount_eok"], ascending=[False, False]).head(15),
            "전기 대비 증감(억원)",
            lambda row: _fmt_num(row["change_eok"]),
            "전기 대비 증감률",
            lambda row: _fmt_pct(row["change_pct"]),
        )
    )
    lines.extend(
        _build_table(
            f"{label} YoY 증감률 Top 15",
            quarter_df.dropna(subset=["yoy_change_pct"]).sort_values(["yoy_change_pct", "amount_eok"], ascending=[False, False]).head(15),
            "YoY 증감(억원)",
            lambda row: _fmt_num(row["yoy_change_eok"]),
            "YoY 증감률",
            lambda row: _fmt_pct(row["yoy_change_pct"]),
        )
    )
    lines.extend(
        _build_table(
            f"{label} 절대 수주잔고 Top 10",
            quarter_df.dropna(subset=["amount_eok"]).sort_values(["amount_eok"], ascending=[False]).head(10),
            "전기 대비 증감률",
            lambda row: _fmt_pct(row["change_pct"]),
            "YoY 증감률",
            lambda row: _fmt_pct(row["yoy_change_pct"]),
        )
    )
    lines.extend(
        _build_table(
            f"{label} 전기 대비 증감액 Top 10",
            quarter_df.dropna(subset=["change_eok"]).sort_values(["change_eok", "amount_eok"], ascending=[False, False]).head(10),
            "전기 대비 증감(억원)",
            lambda row: _fmt_num(row["change_eok"]),
            "전기 대비 증감률",
            lambda row: _fmt_pct(row["change_pct"]),
        )
    )
    lines.extend(
        _build_table(
            f"{label} YoY 증감액 Top 10",
            quarter_df.dropna(subset=["yoy_change_eok"]).sort_values(["yoy_change_eok", "amount_eok"], ascending=[False, False]).head(10),
            "YoY 증감(억원)",
            lambda row: _fmt_num(row["yoy_change_eok"]),
            "YoY 증감률",
            lambda row: _fmt_pct(row["yoy_change_pct"]),
        )
    )
    lines.extend(_build_growth_streak_sections(df, quarter_df, period, label))
    lines.extend(["</details>", ""])
    return lines


def _build_growth_streak_sections(
    df: pd.DataFrame,
    quarter_df: pd.DataFrame,
    period: str,
    label: str,
) -> list[str]:
    lines: list[str] = []
    lines.extend(_build_growth_streak_section(df, quarter_df, period, label, basis="yoy"))
    lines.extend(_build_growth_streak_section(df, quarter_df, period, label, basis="qoq"))
    return lines


def _build_growth_streak_section(
    df: pd.DataFrame,
    quarter_df: pd.DataFrame,
    period: str,
    label: str,
    basis: str,
) -> list[str]:
    streak_df = _build_growth_streak_df(df, quarter_df, period, basis=basis)
    basis_label = "YoY" if basis == "yoy" else "QoQ"
    lines = [f"### {label} {basis_label} 3분기 이상 연속 증가 기업", ""]
    if streak_df.empty:
        lines.extend(["<p>조건을 만족하는 기업 없음</p>", ""])
        return lines

    lines.extend(["<table>", "<thead>"])
    if basis == "yoy":
        lines.append(
            "<tr><th>순위</th><th>기업명</th><th>수주잔고(억원)</th><th>YoY 연속 분기 수</th><th>현재 YoY</th><th>YoY 증감(억원)</th></tr>"
        )
    else:
        lines.append(
            "<tr><th>순위</th><th>기업명</th><th>수주잔고(억원)</th><th>QoQ 연속 분기 수</th><th>현재 QoQ</th><th>전기 대비 증감(억원)</th></tr>"
        )
    lines.extend(["</thead>", "<tbody>"])
    for index, (_, row) in enumerate(streak_df.iterrows(), start=1):
        extra_value = _fmt_num(row["yoy_change_eok"]) if basis == "yoy" else _fmt_num(row["change_eok"])
        lines.append(
            "<tr>"
            f"<td>{index}</td>"
            f"<td>{_company_link(row)}</td>"
            f"<td>{html.escape(_fmt_num(row['amount_eok']))}</td>"
            f"<td>{int(row['streak_quarters'])}</td>"
            f"<td>{html.escape(_fmt_pct(row['basis_pct']))}</td>"
            f"<td>{html.escape(extra_value)}</td>"
            "</tr>"
        )
    lines.extend(["</tbody>", "</table>", ""])
    return lines


def _build_growth_streak_df(
    df: pd.DataFrame,
    quarter_df: pd.DataFrame,
    period: str,
    basis: str,
) -> pd.DataFrame:
    history_df = df.copy()
    history_df["period_key"] = history_df["report_period"].map(_report_period_key)
    target_period_key = _report_period_key(period)
    history_df = history_df.dropna(subset=["period_key"]).copy()
    history_df = history_df.loc[history_df["period_key"] <= target_period_key]
    history_df = history_df.sort_values(["corp_code", "period_key", "filing_date"]).drop_duplicates(
        ["corp_code", "report_period"], keep="last"
    )

    streak_rows: list[dict[str, object]] = []
    for _, current_row in quarter_df.iterrows():
        corp_history = history_df.loc[history_df["corp_code"] == current_row["corp_code"]].sort_values("period_key")
        if corp_history.empty:
            continue
        streak_quarters = _count_growth_streak(corp_history, basis=basis)
        if streak_quarters < GROWTH_STREAK_MIN_QUARTERS:
            continue
        streak_rows.append(
            {
                **current_row.to_dict(),
                "streak_quarters": streak_quarters,
                "basis_pct": _basis_value(current_row, basis),
                "growth_score": _growth_score(current_row, basis),
            }
        )

    if not streak_rows:
        return pd.DataFrame()
    return pd.DataFrame(streak_rows).sort_values(
        ["streak_quarters", "growth_score", "amount_eok"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def _count_growth_streak(corp_history: pd.DataFrame, basis: str) -> int:
    streak = 0
    for _, row in corp_history.sort_values("period_key", ascending=False).iterrows():
        if _is_growth_qualified(row, basis=basis):
            streak += 1
            continue
        break
    return streak


def _is_growth_qualified(row: pd.Series, basis: str) -> bool:
    return _pct_at_least(_basis_value(row, basis), GROWTH_STREAK_THRESHOLD_PCT)


def _pct_at_least(value: float | int | None, threshold: float) -> bool:
    return pd.notna(value) and float(value) >= threshold


def _basis_value(row: pd.Series, basis: str) -> float | int | None:
    if basis == "yoy":
        return row.get("yoy_change_pct")
    return row.get("change_pct")


def _growth_score(row: pd.Series, basis: str) -> float:
    value = _basis_value(row, basis)
    return float(value) if pd.notna(value) else float("-inf")


def _build_table(
    title: str,
    table_df: pd.DataFrame,
    value1_label: str,
    value1_fn,
    value2_label: str,
    value2_fn,
) -> list[str]:
    lines = [f"### {title}", "", "<table>", "<thead>"]
    lines.append(
        "<tr><th>순위</th><th>기업명</th><th>수주잔고(억원)</th><th>{}</th><th>{}</th></tr>".format(
            html.escape(value1_label), html.escape(value2_label)
        )
    )
    lines.extend(["</thead>", "<tbody>"])
    for index, (_, row) in enumerate(table_df.iterrows(), start=1):
        lines.append(
            "<tr>"
            f"<td>{index}</td>"
            f"<td>{_company_link(row)}</td>"
            f"<td>{html.escape(_fmt_num(row['amount_eok']))}</td>"
            f"<td>{html.escape(value1_fn(row))}</td>"
            f"<td>{html.escape(value2_fn(row))}</td>"
            "</tr>"
        )
    lines.extend(["</tbody>", "</table>", ""])
    return lines


def _company_filename(company_name: str, stock_code: str | None) -> str:
    base_name = "\uc2e0\uc131\uc774\uc5d4\uc9c0"
    if company_name.startswith(base_name + "("):
        company_name = base_name
    sanitized_name = "".join(char if char.isalnum() else "_" for char in company_name).strip("_")
    code_text = "".join(char for char in str(stock_code or "-") if char.isalnum()) or "-"
    if code_text.isdigit():
        code_text = code_text.zfill(6)
    return f"{sanitized_name}_수주잔고({code_text}).md"


def _company_link(row: pd.Series) -> str:
    file_name = _company_filename(str(row["corp_name"]), row["stock_code"])
    return f'<a href="{html.escape(file_name)}">{html.escape(str(row["corp_name"]))}</a>'


def _fmt_num(value: float | int | None) -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value):,.2f}".rstrip("0").rstrip(".")


def _fmt_pct(value: float | int | None) -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value):,.2f}%".rstrip("0").rstrip(".")


def _report_period_key(report_period: str) -> int | None:
    report_period_text = str(report_period).strip()
    if not report_period_text or report_period_text.lower() == "nan" or "." not in report_period_text:
        return None
    year_text, month_text = report_period_text.split(".", 1)
    if not (year_text.isdigit() and month_text.isdigit()):
        return None
    return int(year_text) * 100 + int(month_text)


if __name__ == "__main__":
    main()
