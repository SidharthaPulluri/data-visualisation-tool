from __future__ import annotations

from typing import Any

import pandas as pd

from utils.helpers import chart_options_for_type


def _looks_like_datetime(series: pd.Series) -> bool:
    sample = series.dropna()
    if sample.empty:
        return False

    converted = pd.to_datetime(sample.astype(str), errors="coerce", format="mixed")
    return bool((converted.notna().mean()) >= 0.8)


def infer_column_type(series: pd.Series) -> str:
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(series) or _looks_like_datetime(series):
        return "datetime"

    unique_count = series.nunique(dropna=True)
    if unique_count <= max(20, int(len(series) * 0.2)):
        return "categorical"
    return "text"


def detect_schema(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    schema: dict[str, dict[str, Any]] = {}
    for column in df.columns:
        col_type = infer_column_type(df[column])
        schema[column] = {
            "type": col_type,
            "dtype": str(df[column].dtype),
            "missing": int(df[column].isna().sum()),
            "unique": int(df[column].nunique(dropna=True)),
            "allowed_charts": chart_options_for_type(col_type),
        }
    return schema
