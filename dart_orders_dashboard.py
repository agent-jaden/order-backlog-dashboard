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
    lines.extend(["</details>", ""])
    return lines


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


if __name__ == "__main__":
    main()
