from __future__ import annotations

from typing import Iterable

import pandas as pd


FIELD_ALIASES = {
    "country_code": ["cntyCd"],
    "country_name": ["cntyKorNm", "cntyEngNm"],
    "hs_code": ["hsSgn"],
    "item_name": ["hsKorNm", "hsEngNm"],
    "export_usd": ["expDlr", "expdlr"],
    "import_usd": ["impDlr", "impdlr"],
    "trade_balance_usd": ["balPayments", "balpayments"],
    "weight_kg": ["netWgt", "wgt"],
}


def normalize_trade_frame(records: Iterable[dict], dataset: str) -> pd.DataFrame:
    df = pd.DataFrame(list(records))
    if df.empty:
        return df

    normalized = pd.DataFrame()
    normalized["dataset"] = dataset
    normalized["period"] = _build_period(df)

    for target_column, aliases in FIELD_ALIASES.items():
        normalized[target_column] = _pick_first_existing_column(df, aliases)

    for column in ["export_usd", "import_usd", "trade_balance_usd", "weight_kg"]:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized["period"] = pd.to_datetime(normalized["period"], format="%Y%m")
    return normalized.sort_values("period").reset_index(drop=True)


def calculate_growth_metrics(
    normalized_df: pd.DataFrame,
    group_columns: list[str] | None = None,
) -> pd.DataFrame:
    if normalized_df.empty:
        return normalized_df

    group_columns = group_columns or _default_group_columns(normalized_df)
    metrics_df = normalized_df.copy().sort_values(group_columns + ["period"])

    for value_column in ["export_usd", "import_usd", "trade_balance_usd", "weight_kg"]:
        if value_column not in metrics_df.columns:
            continue
        metrics_df[f"{value_column}_mom_pct"] = (
            metrics_df.groupby(group_columns, dropna=False)[value_column].pct_change() * 100
        )
        metrics_df[f"{value_column}_mom_abs"] = metrics_df.groupby(group_columns, dropna=False)[
            value_column
        ].diff()

    return metrics_df


def summarize_latest_changes(metrics_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    if metrics_df.empty:
        return metrics_df

    latest_period = metrics_df["period"].max()
    latest_rows = metrics_df.loc[metrics_df["period"] == latest_period].copy()
    sort_column = "export_usd_mom_pct" if "export_usd_mom_pct" in latest_rows.columns else "period"
    return latest_rows.sort_values(sort_column, ascending=False).head(top_n)


def _build_period(df: pd.DataFrame) -> pd.Series:
    if "yymm" in df.columns:
        return df["yymm"]
    if "year" in df.columns and "month" in df.columns:
        year = df["year"].astype(str).str.zfill(4)
        month = df["month"].astype(str).str.zfill(2)
        return year + month
    if "strtYymm" in df.columns:
        return df["strtYymm"]
    raise ValueError("Could not identify a period column in the API response.")


def _pick_first_existing_column(df: pd.DataFrame, aliases: list[str]) -> pd.Series:
    for alias in aliases:
        if alias in df.columns:
            return df[alias]
    return pd.Series([None] * len(df))


def _default_group_columns(df: pd.DataFrame) -> list[str]:
    candidates = ["dataset", "country_code", "country_name", "hs_code", "item_name"]
    selected = [column for column in candidates if column in df.columns]
    return selected or ["dataset"]

