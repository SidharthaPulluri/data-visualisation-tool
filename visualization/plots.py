from __future__ import annotations

from io import BytesIO
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set_theme(style="whitegrid")


def _validate_chart(schema: dict[str, dict[str, str]], chart_type: str, x_column: str, y_column: str | None) -> None:
    if x_column not in schema:
        raise ValueError("Selected X column is not available in the transformed dataset.")

    x_type = schema[x_column]["type"]
    y_type = schema[y_column]["type"] if y_column and y_column in schema else None

    rules = {
        "bar": x_type in {"categorical", "datetime", "text"} or (y_type == "numeric"),
        "pie": x_type in {"categorical", "text"} or (y_type == "numeric"),
        "histogram": x_type == "numeric",
        "box": x_type == "numeric",
        "line": x_type == "datetime" and y_type == "numeric",
        "scatter": x_type == "numeric" and y_type == "numeric",
    }

    if not rules.get(chart_type, False):
        raise ValueError(f"The '{chart_type}' chart does not match the selected column types.")


def create_chart(
    df: pd.DataFrame,
    schema: dict[str, dict[str, str]],
    chart_type: str,
    x_column: str,
    y_column: str | None,
    title: str | None,
    output_format: str = "png",
) -> bytes:
    _validate_chart(schema, chart_type, x_column, y_column)

    figure, axis = plt.subplots(figsize=(10, 6))

    if chart_type == "bar":
        if y_column and y_column in df.columns:
            sns.barplot(data=df, x=x_column, y=y_column, ax=axis, errorbar=None)
        else:
            counts = df[x_column].astype(str).value_counts().head(20)
            sns.barplot(x=counts.index, y=counts.values, ax=axis)
            axis.set_ylabel("count")
    elif chart_type == "pie":
        if y_column and y_column in df.columns:
            series = df[[x_column, y_column]].dropna().head(12)
            axis.pie(series[y_column], labels=series[x_column], autopct="%1.1f%%", startangle=90)
        else:
            counts = df[x_column].astype(str).value_counts().head(10)
            axis.pie(counts.values, labels=counts.index, autopct="%1.1f%%", startangle=90)
        axis.axis("equal")
    elif chart_type == "histogram":
        sns.histplot(data=df, x=x_column, kde=False, bins=20, ax=axis)
    elif chart_type == "box":
        sns.boxplot(data=df, y=x_column, ax=axis)
        axis.set_xlabel("")
    elif chart_type == "line":
        ordered = df.sort_values(by=x_column)
        sns.lineplot(data=ordered, x=x_column, y=y_column, ax=axis, marker="o")
    elif chart_type == "scatter":
        sns.scatterplot(data=df, x=x_column, y=y_column, ax=axis)
    else:
        raise ValueError(f"Unsupported chart type: {chart_type}")

    axis.set_title(title or f"{chart_type.title()} chart")
    axis.tick_params(axis="x", rotation=20)
    figure.tight_layout()
    buffer = BytesIO()
    figure.savefig(buffer, dpi=180, bbox_inches="tight", format=output_format)
    plt.close(figure)
    buffer.seek(0)
    return buffer.getvalue()
