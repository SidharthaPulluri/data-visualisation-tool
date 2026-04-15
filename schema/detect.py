from __future__ import annotations

import re
from typing import Any

import pandas as pd

from utils.helpers import chart_options_for_type


def _looks_like_datetime(series: pd.Series) -> bool:
    sample = series.dropna()
    if sample.empty:
        return False

    converted = pd.to_datetime(sample.astype(str), errors="coerce", format="mixed")
    return bool((converted.notna().mean()) >= 0.8)


def _tokenize_column_name(column_name: str) -> list[str]:
    return [token for token in re.split(r"[_\W]+", column_name.lower()) if token]


def _looks_like_identifier(column_name: str, series: pd.Series, unique_ratio: float) -> bool:
    tokens = _tokenize_column_name(column_name)
    identifier_tokens = {"id", "code", "uuid", "key", "index"}
    return bool(identifier_tokens.intersection(tokens)) and unique_ratio >= 0.85


def _looks_like_rate(column_name: str) -> bool:
    tokens = _tokenize_column_name(column_name)
    rate_tokens = {"percent", "rate", "ratio", "prevalence", "share"}
    return bool(rate_tokens.intersection(tokens)) or "per_100_000" in column_name.lower()


def _looks_like_count(column_name: str) -> bool:
    tokens = _tokenize_column_name(column_name)
    count_tokens = {"count", "counts", "cases", "deaths", "incidence", "population", "total", "number"}
    return bool(count_tokens.intersection(tokens))


def _looks_like_geo(column_name: str) -> bool:
    tokens = _tokenize_column_name(column_name)
    geo_tokens = {"country", "territory", "region", "state", "province", "city", "district"}
    return bool(geo_tokens.intersection(tokens))


def _looks_like_time_dimension(column_name: str) -> bool:
    tokens = _tokenize_column_name(column_name)
    time_tokens = {"year", "date", "month", "day", "quarter", "week"}
    return bool(time_tokens.intersection(tokens))


def infer_column_type(series: pd.Series) -> str:
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(series) or _looks_like_datetime(series):
        return "datetime"

    unique_count = series.nunique(dropna=True)
    if unique_count <= max(20, int(len(series) * 0.2)):
        return "categorical"
    return "text"


def _infer_column_role(column_name: str, series: pd.Series, column_type: str, unique_ratio: float) -> str:
    if series.nunique(dropna=True) <= 1:
        return "constant"
    if column_type == "datetime" or _looks_like_time_dimension(column_name):
        return "time"
    if _looks_like_identifier(column_name, series, unique_ratio):
        return "identifier"
    if _looks_like_geo(column_name):
        return "geography"
    if column_type == "numeric" and _looks_like_rate(column_name):
        return "rate"
    if column_type == "numeric" and _looks_like_count(column_name):
        return "count"
    if column_type == "numeric":
        return "measure"
    if column_type == "categorical":
        return "category"
    return "descriptor"


def _aggregation_hint(role: str) -> str:
    hints = {
        "identifier": "exclude from aggregations",
        "constant": "exclude from charts",
        "time": "use as timeline or latest snapshot",
        "geography": "group and compare categories",
        "rate": "average or use latest value",
        "count": "sum across groups",
        "measure": "average or compare distribution",
        "category": "count groups or compare with numeric measures",
        "descriptor": "use as labels or categories after filtering",
    }
    return hints.get(role, "review manually")


def _allowed_charts(column_type: str, role: str, unique_count: int) -> list[str]:
    if role in {"identifier", "constant"}:
        return []
    charts = chart_options_for_type(column_type)
    if role == "geography" and "pie" in charts and unique_count > 8:
        charts = [chart for chart in charts if chart != "pie"]
    if role == "descriptor":
        charts = [chart for chart in charts if chart != "pie"]
    return charts


def _column_warnings(column_name: str, series: pd.Series, column_type: str, role: str, missing_count: int) -> list[str]:
    warnings: list[str] = []
    unique_count = int(series.nunique(dropna=True))
    unique_ratio = unique_count / max(len(series), 1)

    if role == "identifier":
        warnings.append("Looks like an identifier; avoid using it as a measure.")
    if role == "constant":
        warnings.append("Only one unique value; this column will not be informative in charts.")
    if column_type in {"categorical", "text"} and unique_count > 12:
        warnings.append("Many unique labels; prefer bar charts with filters over pie charts.")
    if column_type == "numeric" and unique_ratio <= 0.03 and role not in {"rate", "count"}:
        warnings.append("Very low numeric variety; this may behave more like a category than a measure.")
    if missing_count > len(series) * 0.3:
        warnings.append("High missing-value rate; results may need careful interpretation.")
    if role == "rate":
        warnings.append("Rate-like metric; average or latest value is usually better than sum.")
    return warnings


def detect_schema(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    schema: dict[str, dict[str, Any]] = {}
    row_count = max(len(df), 1)

    for column in df.columns:
        series = df[column]
        column_type = infer_column_type(series)
        missing = int(series.isna().sum())
        unique = int(series.nunique(dropna=True))
        unique_ratio = round(unique / row_count, 4)
        role = _infer_column_role(column, series, column_type, unique_ratio)
        schema[column] = {
            "type": column_type,
            "role": role,
            "dtype": str(series.dtype),
            "missing": missing,
            "unique": unique,
            "unique_ratio": unique_ratio,
            "completeness": round(1 - (missing / row_count), 4),
            "aggregation_hint": _aggregation_hint(role),
            "allowed_charts": _allowed_charts(column_type, role, unique),
            "warnings": _column_warnings(column, series, column_type, role, missing),
        }
    return schema
