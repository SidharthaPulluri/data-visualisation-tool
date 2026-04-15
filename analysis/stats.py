from __future__ import annotations

import re
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


def _is_identifier_column(column_name: str, series: pd.Series) -> bool:
    normalized = column_name.strip().lower()
    tokens = [token for token in re.split(r"[_\W]+", normalized) if token]
    unique_ratio = series.nunique(dropna=True) / max(len(series), 1)
    return "id" in tokens and unique_ratio >= 0.9


def _top_correlation_pairs(df: pd.DataFrame, numeric_columns: list[str], limit: int = 10) -> list[dict[str, Any]]:
    if len(numeric_columns) < 2:
        return []

    correlation_frame = df[numeric_columns].corr(numeric_only=True).fillna(0)
    pairs: list[dict[str, Any]] = []
    for index, left in enumerate(numeric_columns):
        for right in numeric_columns[index + 1 :]:
            value = float(correlation_frame.loc[left, right])
            pairs.append(
                {
                    "left": left,
                    "right": right,
                    "value": round(value, 4),
                    "absolute_value": round(abs(value), 4),
                }
            )

    pairs.sort(key=lambda item: item["absolute_value"], reverse=True)
    return pairs[:limit]


def _build_group_comparison(
    df: pd.DataFrame, categorical_columns: list[str], numeric_columns: list[str]
) -> dict[str, Any] | None:
    if not categorical_columns or not numeric_columns:
        return None

    for column in categorical_columns:
        categories = df[column].dropna().astype(str).unique()
        if 1 < len(categories) <= 10:
            grouped = df.groupby(column, dropna=False)[numeric_columns].mean(numeric_only=True)
            feature_gaps: list[dict[str, Any]] = []
            for metric in numeric_columns:
                series = grouped[metric].dropna()
                if series.empty:
                    continue

                feature_gaps.append(
                    {
                        "metric": metric,
                        "lowest_group": str(series.idxmin()),
                        "lowest_mean": round(float(series.min()), 4),
                        "highest_group": str(series.idxmax()),
                        "highest_mean": round(float(series.max()), 4),
                        "difference": round(float(series.max() - series.min()), 4),
                    }
                )

            feature_gaps.sort(key=lambda item: abs(item["difference"]), reverse=True)
            return {
                "group_column": column,
                "group_sizes": df[column].astype(str).value_counts(dropna=False).to_dict(),
                "largest_mean_gaps": feature_gaps[:5],
            }

    return None


def build_analysis_report(df: pd.DataFrame, schema: dict[str, dict[str, Any]]) -> dict[str, Any]:
    numeric_columns = [name for name, meta in schema.items() if meta["type"] == "numeric"]
    categorical_columns = [name for name, meta in schema.items() if meta["type"] == "categorical"]
    datetime_columns = [name for name, meta in schema.items() if meta["type"] == "datetime"]
    text_columns = [name for name, meta in schema.items() if meta["type"] == "text"]
    identifier_columns = [
        name for name, meta in schema.items() if meta.get("role") == "identifier"
    ] or [name for name in numeric_columns if _is_identifier_column(name, df[name])]
    analysis_numeric_columns = [
        name
        for name in numeric_columns
        if name not in identifier_columns and schema[name].get("role") != "constant"
    ]
    rate_columns = [name for name, meta in schema.items() if meta.get("role") == "rate"]
    count_columns = [name for name, meta in schema.items() if meta.get("role") == "count"]
    geography_columns = [name for name, meta in schema.items() if meta.get("role") == "geography"]

    numeric_summary = {}
    summary_columns = analysis_numeric_columns[:8]
    if summary_columns:
        numeric_summary = _round_nested(df[summary_columns].describe().to_dict())

    top_correlations = _top_correlation_pairs(df, analysis_numeric_columns)

    categorical_summary = {
        column: df[column].astype(str).value_counts(dropna=False).head(10).to_dict()
        for column in categorical_columns[:5]
    }
    group_comparison = _build_group_comparison(df, categorical_columns, analysis_numeric_columns)

    summary_lines = [
        f"Numeric columns: {len(numeric_columns)}",
        f"Categorical columns: {len(categorical_columns)}",
        f"Rows analyzed: {len(df)}",
    ]
    if analysis_numeric_columns:
        summary_lines.append(f"Primary numeric fields: {', '.join(analysis_numeric_columns[:5])}")
    if categorical_columns:
        summary_lines.append(f"Categorical fields: {', '.join(categorical_columns[:5])}")
    if identifier_columns:
        summary_lines.append(f"Excluded identifier-like fields from deep stats: {', '.join(identifier_columns)}")
    if rate_columns:
        summary_lines.append(f"Rate-like fields: {', '.join(rate_columns[:5])}")
    if count_columns:
        summary_lines.append(f"Count-like fields: {', '.join(count_columns[:5])}")

    key_insights = []
    if top_correlations:
        strongest = top_correlations[0]
        key_insights.append(
            f"Strongest relationship: {strongest['left']} vs {strongest['right']} ({strongest['value']})."
        )
    if group_comparison and group_comparison["largest_mean_gaps"]:
        standout = group_comparison["largest_mean_gaps"][0]
        key_insights.append(
            f"Largest group gap in {group_comparison['group_column']}: {standout['metric']} differs by "
            f"{standout['difference']} between {standout['highest_group']} and {standout['lowest_group']}."
        )
    if not key_insights:
        key_insights.append("No standout relationships were available for this dataset shape.")

    warnings: list[str] = []
    if geography_columns and len(df) > 500:
        warnings.append(
            f"Geography-like columns such as {geography_columns[0]} may need top-N filtering for readable pie or bar charts."
        )
    for column, meta in schema.items():
        for warning in meta.get("warnings", [])[:1]:
            warnings.append(f"{column}: {warning}")
        if len(warnings) >= 5:
            break

    return {
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "type_counts": {
            "numeric": len(numeric_columns),
            "categorical": len(categorical_columns),
            "datetime": len(datetime_columns),
            "text": len(text_columns),
        },
        "primary_numeric_fields": analysis_numeric_columns[:5],
        "categorical_fields": categorical_columns[:5],
        "numeric_summary": numeric_summary,
        "top_correlations": top_correlations,
        "categorical_breakdown": categorical_summary,
        "group_comparison": group_comparison,
        "key_insights": key_insights,
        "warnings": warnings,
        "identifier_columns": identifier_columns,
        "summary_lines": "\n".join(summary_lines),
    }
