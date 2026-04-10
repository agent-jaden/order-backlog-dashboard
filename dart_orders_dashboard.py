from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from dart_orders_timeseries import _default_output_path


QUARTER_SPECS = [
    ("2025.12", "사업보고서", "2025년 4분기"),
    ("2025.09", "분기보고서", "2025년 3분기"),
    ("2025.06", "반기보고서", "2025년 2분기"),
    ("2025.03", "분기보고서", "2025년 1분기"),
]


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

    df = pd.read_csv(input_csv, dtype={"corp_code": str, "stock_code": str})
    for column in ["amount_eok", "change_eok", "change_pct", "yoy_change_eok", "yoy_change_pct"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    lines = [
        "# 수주잔고 대시보드",
        "",
        f"- 기준 데이터: `{input_csv.as_posix()}`",
        "- 기준 범위: `2025년 1분기 ~ 2025년 4분기`",
        "- 기업명 링크: 각 기업별 수주잔고 MD 파일로 연결",
        "- 주의: 자동 추출 결과이므로 일부 극단값은 이상치 점검이 필요할 수 있음",
        "",
    ]

    for index, (period, report_kind, label) in enumerate(QUARTER_SPECS):
        lines.extend(_build_quarter_section(df, period, report_kind, label, open_by_default=index == 0))

    lines.extend(
        [
            "## 메모",
            "",
            "- 이 순위는 자동 추출된 시계열 기준의 단순 정렬 결과임",
            "- 극단적인 증가율이나 금액은 분모가 매우 작은 경우, 단위 인식 문제, 합계 선택 문제 때문에 과대하게 보일 수 있음",
            "- 실제 해석 전에는 `수주잔고_이상치점검표`와 개별 기업 MD를 같이 확인하는 것이 안전함",
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
        lines.append("- 데이터 없음")
        lines.append("")
        lines.append("</details>")
        lines.append("")
        return lines

    lines.extend(
        _build_table(
            f"{label} 전기 대비 증감률 Top 10",
            quarter_df.dropna(subset=["change_pct"]).sort_values(["change_pct", "amount_eok"], ascending=[False, False]).head(10),
            "전기 대비 증감(억원)",
            lambda row: _fmt_num(row["change_eok"]),
            "전기 대비 증감률",
            lambda row: _fmt_pct(row["change_pct"]),
        )
    )
    lines.extend(
        _build_table(
            f"{label} YoY 증감률 Top 10",
            quarter_df.dropna(subset=["yoy_change_pct"]).sort_values(["yoy_change_pct", "amount_eok"], ascending=[False, False]).head(10),
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
    lines.append("</details>")
    lines.append("")
    return lines


def _build_table(
    title: str,
    table_df: pd.DataFrame,
    value1_label: str,
    value1_fn,
    value2_label: str,
    value2_fn,
) -> list[str]:
    lines = [f"### {title}", ""]
    lines.append(f"| 순위 | 기업명 | 수주잔고(억원) | {value1_label} | {value2_label} |")
    lines.append("| --- | --- | ---: | ---: | ---: |")
    for index, (_, row) in enumerate(table_df.iterrows(), start=1):
        lines.append(
            f"| {index} | {_company_link(row)} | {_fmt_num(row['amount_eok'])} | {value1_fn(row)} | {value2_fn(row)} |"
        )
    lines.append("")
    return lines


def _company_link(row: pd.Series) -> str:
    md_path = Path("D:/GPT Codex") / _default_output_path(str(row["corp_name"]), str(row["stock_code"]).zfill(6))
    return f"[{row['corp_name']}]({md_path})"


def _fmt_num(value: float | int | None) -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value):,.2f}".rstrip("0").rstrip(".")


def _fmt_pct(value: float | int | None) -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value):,.2f}%"


if __name__ == "__main__":
    main()
