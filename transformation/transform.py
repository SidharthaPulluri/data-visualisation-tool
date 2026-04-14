from __future__ import annotations

from typing import Any

import pandas as pd


FILTER_OPERATORS = {
    "==": lambda series, value: series == value,
    "!=": lambda series, value: series != value,
    ">": lambda series, value: series > value,
    ">=": lambda series, value: series >= value,
    "<": lambda series, value: series < value,
    "<=": lambda series, value: series <= value,
    "contains": lambda series, value: series.astype(str).str.contains(str(value), case=False, na=False),
}


def _coerce_value(series: pd.Series, raw_value: Any) -> Any:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(raw_value, errors="coerce")
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(raw_value, errors="coerce")
    return str(raw_value)


def _apply_derived_columns(df: pd.DataFrame, derived_columns: list[dict[str, Any]], steps: list[str]) -> pd.DataFrame:
    transformed = df.copy()
    operations = {
        "add": lambda a, b: a + b,
        "subtract": lambda a, b: a - b,
        "multiply": lambda a, b: a * b,
        "divide": lambda a, b: a / b,
    }

    for item in derived_columns:
        name = item.get("name")
        left = item.get("left")
        operation = item.get("operation")
        right = item.get("right")

        if not name or left not in transformed.columns or operation not in operations:
            continue

        left_series = pd.to_numeric(transformed[left], errors="coerce")
        if right in transformed.columns:
            right_value = pd.to_numeric(transformed[right], errors="coerce")
        else:
            right_value = pd.to_numeric(right, errors="coerce")

        transformed[name] = operations[operation](left_series, right_value)
        steps.append(f"Derived column '{name}' created with {left} {operation} {right}.")

    return transformed


def _apply_filters(df: pd.DataFrame, filters: list[dict[str, Any]], steps: list[str]) -> pd.DataFrame:
    filtered = df.copy()
    for item in filters:
        column = item.get("column")
        operator = item.get("operator")
        value = item.get("value")
        if column not in filtered.columns or operator not in FILTER_OPERATORS or value in (None, ""):
            continue

        typed_value = _coerce_value(filtered[column], value)
        filtered = filtered[FILTER_OPERATORS[operator](filtered[column], typed_value)]
        steps.append(f"Filtered rows where {column} {operator} {value}.")
    return filtered


def _apply_groupby(df: pd.DataFrame, config: dict[str, Any], steps: list[str]) -> pd.DataFrame:
    group_by = [column for column in config.get("group_by", []) if column in df.columns]
    aggregations = config.get("aggregations", [])
    if not group_by or not aggregations:
        return df

    agg_map: dict[str, str] = {}
    for item in aggregations:
        column = item.get("column")
        operation = item.get("operation")
        if column in df.columns and operation in {"sum", "mean", "count", "min", "max"}:
            agg_map[column] = operation

    if not agg_map:
        return df

    grouped = df.groupby(group_by, dropna=False).agg(agg_map).reset_index()
    steps.append(
        f"Grouped by {', '.join(group_by)} with aggregations: "
        + ", ".join(f"{column}={operation}" for column, operation in agg_map.items())
        + "."
    )
    return grouped


def apply_transformations(df: pd.DataFrame, config: dict[str, Any]) -> tuple[pd.DataFrame, list[str]]:
    transformed = df.copy()
    steps: list[str] = []

    transformed = _apply_derived_columns(transformed, config.get("derived_columns", []), steps)
    transformed = _apply_filters(transformed, config.get("filters", []), steps)
    transformed = _apply_groupby(transformed, config, steps)

    selected_columns = [column for column in config.get("selected_columns", []) if column in transformed.columns]
    if selected_columns:
        transformed = transformed[selected_columns]
        steps.append(f"Selected columns: {', '.join(selected_columns)}.")

    return transformed, steps
