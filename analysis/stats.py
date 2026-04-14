from __future__ import annotations

from typing import Any

import pandas as pd


def _round_nested(data: Any) -> Any:
    if isinstance(data, dict):
        return {key: _round_nested(value) for key, value in data.items()}
    if isinstance(data, list):
        return [_round_nested(item) for item in data]
    if isinstance(data, float):
        return round(data, 4)
    return data


def build_analysis_report(df: pd.DataFrame, schema: dict[str, dict[str, Any]]) -> dict[str, Any]:
    numeric_columns = [name for name, meta in schema.items() if meta["type"] == "numeric"]
    categorical_columns = [name for name, meta in schema.items() if meta["type"] == "categorical"]

    numeric_summary = {}
    if numeric_columns:
        numeric_summary = _round_nested(df[numeric_columns].describe().to_dict())

    correlations = {}
    if len(numeric_columns) >= 2:
        correlations = _round_nested(df[numeric_columns].corr(numeric_only=True).fillna(0).to_dict())

    categorical_summary = {
        column: df[column].astype(str).value_counts(dropna=False).head(10).to_dict()
        for column in categorical_columns[:5]
    }

    summary_lines = [
        f"Numeric columns: {len(numeric_columns)}",
        f"Categorical columns: {len(categorical_columns)}",
        f"Rows analyzed: {len(df)}",
    ]
    if numeric_columns:
        summary_lines.append(f"Top numeric fields: {', '.join(numeric_columns[:5])}")
    if categorical_columns:
        summary_lines.append(f"Top categorical fields: {', '.join(categorical_columns[:5])}")

    return {
        "numeric_summary": numeric_summary,
        "correlations": correlations,
        "categorical_breakdown": categorical_summary,
        "summary_lines": "\n".join(summary_lines),
    }
