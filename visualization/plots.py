from __future__ import annotations

from io import BytesIO
from textwrap import wrap
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set_theme(style="whitegrid")


def _wrap_label(value: object, width: int = 18) -> str:
    text = str(value)
    if len(text) <= width:
        return text
    return "\n".join(wrap(text, width=width, break_long_words=True, break_on_hyphens=False))


def _has_long_labels(values: list[object], limit: int = 18) -> bool:
    return any(len(str(value)) > limit for value in values)


def _set_categorical_figure_size(figure: plt.Figure, labels: list[object], horizontal: bool) -> None:
    label_lengths = [len(str(label)) for label in labels] or [0]
    max_length = max(label_lengths)
    count = max(len(labels), 1)
    if horizontal:
        width = min(max(9.0, 8.5 + max_length * 0.08), 16.0)
        height = min(max(4.8, 2.8 + count * 0.45), 14.0)
    else:
        width = min(max(9.0, 5.5 + count * 0.45), 16.0)
        height = min(max(6.0, 5.2 + max_length * 0.04), 11.0)
    figure.set_size_inches(width, height)


def _safe_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


def _safe_nonnegative_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else default
    except (TypeError, ValueError):
        return default


def _normalise_chart_options(chart_options: dict[str, Any] | None) -> dict[str, Any]:
    options = chart_options or {}
    aggregation = str(options.get("aggregation", "auto")).lower()
    if aggregation not in {"auto", "sum", "mean", "median", "count", "latest"}:
        aggregation = "auto"

    sort_order = str(options.get("sort_order", "desc")).lower()
    if sort_order not in {"desc", "asc", "none"}:
        sort_order = "desc"

    return {
        "aggregation": aggregation,
        "sort_order": sort_order,
        "top_n": _safe_int(options.get("top_n"), 12),
        "row_column": options.get("row_column") or None,
        "bins": _safe_int(options.get("bins"), 20),
        "palette": str(options.get("palette", "blue")).lower(),
        "label_rotation": options.get("label_rotation", "auto"),
        "decimal_places": min(_safe_nonnegative_int(options.get("decimal_places"), 2), 4),
        "show_value_labels": bool(options.get("show_value_labels", False)),
    }


def _palette_colors(name: str) -> tuple[str, str]:
    palettes = {
        "blue": ("#2f67ff", "#9db8ff"),
        "green": ("#1f8f5f", "#97d6b8"),
        "coral": ("#dc6a4d", "#f0b5a5"),
        "slate": ("#42526e", "#a8b3c5"),
    }
    return palettes.get(name, palettes["blue"])


def _resolve_rotation(value: Any, default: int) -> int:
    if value == "auto":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _add_value_labels(axis: plt.Axes, decimals: int = 2, horizontal: bool = False) -> None:
    for patch in axis.patches:
        if horizontal:
            width = patch.get_width()
            y = patch.get_y() + patch.get_height() / 2
            axis.text(width, y, f" {width:.{decimals}f}".rstrip("0").rstrip("."), va="center", ha="left", fontsize=8.5, color="#42526e")
        else:
            height = patch.get_height()
            x = patch.get_x() + patch.get_width() / 2
            axis.text(x, height, f"{height:.{decimals}f}".rstrip("0").rstrip("."), va="bottom", ha="center", fontsize=8.5, color="#42526e")


def _aggregation_function(name: str) -> str:
    mapping = {
        "sum": "sum",
        "mean": "mean",
        "median": "median",
        "count": "count",
    }
    return mapping.get(name, "mean")


def _resolve_aggregation(
    chart_type: str,
    schema: dict[str, dict[str, str]],
    x_column: str,
    y_column: str | None,
    aggregation: str,
) -> str:
    if aggregation != "auto":
        return aggregation

    y_role = schema.get(y_column or "", {}).get("role")
    x_role = schema.get(x_column, {}).get("role")

    if chart_type == "pie":
        return "count" if not y_column else ("mean" if y_role == "rate" else "sum")
    if chart_type == "line":
        return "mean" if y_role == "rate" else "sum" if y_role == "count" else "mean"
    if chart_type == "heatmap":
        if not y_column:
            return "count"
        return "mean" if y_role == "rate" else "sum" if y_role == "count" else "mean"
    if chart_type == "bar":
        if not y_column:
            return "count"
        if x_role == "time":
            return "mean" if y_role == "rate" else "sum" if y_role == "count" else "mean"
        return "mean" if y_role in {"rate", "measure"} else "sum"
    return "mean"


def _latest_metric_by_category(df: pd.DataFrame, category_column: str, value_column: str) -> pd.Series | None:
    time_candidates = [column for column in ("Year", "year", "Date", "date") if column in df.columns and column != category_column]
    if not time_candidates:
        return None

    time_column = time_candidates[0]
    candidates = df[[category_column, value_column, time_column]].dropna()
    if candidates.empty:
        return None

    candidates = candidates.copy()
    candidates[time_column] = pd.to_datetime(candidates[time_column], errors="coerce", format="mixed")
    if candidates[time_column].isna().all():
        candidates[time_column] = pd.to_numeric(candidates[time_column], errors="coerce")
    candidates = candidates.dropna(subset=[time_column])
    if candidates.empty:
        return None

    latest_value = candidates.groupby(category_column, dropna=False)[time_column].transform("max")
    latest = candidates.loc[candidates[time_column] == latest_value]
    if latest.empty:
        return None
    return latest.groupby(category_column, dropna=False)[value_column].mean().sort_values(ascending=False)


def _series_from_grouped_data(
    df: pd.DataFrame,
    schema: dict[str, dict[str, str]],
    x_column: str,
    y_column: str | None,
    chart_type: str,
    options: dict[str, Any],
) -> pd.Series:
    aggregation = _resolve_aggregation(chart_type, schema, x_column, y_column, options["aggregation"])

    if y_column and y_column in df.columns:
        candidates = df[[x_column, y_column]].dropna()
        if candidates.empty:
            raise ValueError("The selected columns do not contain enough data for this chart.")

        if aggregation == "latest":
            latest = _latest_metric_by_category(df, x_column, y_column)
            if latest is None or latest.empty:
                raise ValueError("Latest-value aggregation needs a usable time column such as Year or Date.")
            series = latest
        else:
            grouped = candidates.groupby(x_column, dropna=False)[y_column]
            series = grouped.agg(_aggregation_function(aggregation))
    else:
        series = df[x_column].astype(str).value_counts(dropna=False)

    series = series.dropna()
    if chart_type == "pie":
        series = series[series > 0]
    return series


def _sort_and_trim_series(series: pd.Series, sort_order: str, top_n: int) -> pd.Series:
    cleaned = series.dropna()
    if sort_order == "asc":
        cleaned = cleaned.sort_values(ascending=True)
    elif sort_order == "desc":
        cleaned = cleaned.sort_values(ascending=False)
    if top_n and top_n > 0:
        cleaned = cleaned.head(top_n)
    return cleaned


def _collapse_pie_series(series: pd.Series, top_n: int = 8) -> pd.Series:
    cleaned = series.dropna().sort_values(ascending=False)
    if cleaned.empty or len(cleaned) <= top_n:
        return cleaned
    top = cleaned.iloc[:top_n].copy()
    other_total = cleaned.iloc[top_n:].sum()
    if float(other_total) > 0:
        top.loc["Other"] = other_total
    return top


def _prepare_pie_series(
    df: pd.DataFrame,
    schema: dict[str, dict[str, str]],
    x_column: str,
    y_column: str | None,
    options: dict[str, Any],
) -> pd.Series:
    series = _series_from_grouped_data(df, schema, x_column, y_column, "pie", options)
    if series.empty or len(series) < 2:
        raise ValueError("A pie chart needs at least two non-empty categories.")

    collapsed = _collapse_pie_series(series, top_n=min(options["top_n"], 8))
    total = float(collapsed.sum()) if len(collapsed) else 0.0
    other_share = float(collapsed.get("Other", 0.0)) / total if total else 0.0
    if other_share > 0.7:
        raise ValueError(
            "Pie chart is not a good fit for this selection because too many categories collapse into 'Other'. "
            "Try a bar chart, filter the data, or lower the top-N setting."
        )
    return collapsed


def _prepare_line_frame(
    df: pd.DataFrame,
    schema: dict[str, dict[str, str]],
    x_column: str,
    y_column: str,
    options: dict[str, Any],
) -> pd.DataFrame:
    candidates = df[[x_column, y_column]].dropna()
    if candidates.empty:
        raise ValueError("The selected columns do not contain enough data for a line chart.")

    aggregation = _resolve_aggregation("line", schema, x_column, y_column, options["aggregation"])
    if aggregation == "latest":
        aggregation = "mean"

    grouped = candidates.groupby(x_column, dropna=False)[y_column].agg(_aggregation_function(aggregation)).reset_index()
    x_meta = schema.get(x_column, {})
    if x_meta.get("type") == "datetime" or x_meta.get("role") == "time":
        converted = pd.to_datetime(grouped[x_column], errors="coerce", format="mixed")
        if converted.notna().any():
            grouped = grouped.assign(_sort_x=converted).sort_values("_sort_x").drop(columns="_sort_x")
        else:
            numeric = pd.to_numeric(grouped[x_column], errors="coerce")
            grouped = grouped.assign(_sort_x=numeric).sort_values("_sort_x").drop(columns="_sort_x")
    else:
        grouped = grouped.sort_values(x_column)
    return grouped


def _prepare_heatmap_table(
    df: pd.DataFrame,
    schema: dict[str, dict[str, str]],
    x_column: str,
    row_column: str,
    value_column: str | None,
    options: dict[str, Any],
) -> pd.DataFrame:
    columns = [x_column, row_column] + ([value_column] if value_column else [])
    candidates = df[columns].dropna()
    if candidates.empty:
        raise ValueError("The selected columns do not contain enough data for a heatmap.")

    aggregation = _resolve_aggregation("heatmap", schema, x_column, value_column, options["aggregation"])
    if value_column:
        if aggregation == "latest":
            aggregation = "mean"
        pivot = pd.pivot_table(
            candidates,
            index=row_column,
            columns=x_column,
            values=value_column,
            aggfunc=_aggregation_function(aggregation),
            fill_value=0,
        )
    else:
        pivot = pd.pivot_table(
            candidates.assign(_row_count=1),
            index=row_column,
            columns=x_column,
            values="_row_count",
            aggfunc="sum",
            fill_value=0,
        )

    if pivot.empty:
        raise ValueError("The selected columns did not produce any values for a heatmap.")

    row_totals = pivot.sum(axis=1)
    if options["sort_order"] == "asc":
        pivot = pivot.loc[row_totals.sort_values(ascending=True).index]
    elif options["sort_order"] == "desc":
        pivot = pivot.loc[row_totals.sort_values(ascending=False).index]

    if options["top_n"]:
        pivot = pivot.head(options["top_n"])

    return pivot


def _to_serialisable_scalar(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _format_value_label(value: Any, decimals: int = 2) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.{decimals}f}".rstrip("0").rstrip(".")
    return str(value)


def describe_chart_data(
    df: pd.DataFrame,
    schema: dict[str, dict[str, str]],
    chart_type: str,
    x_column: str,
    y_column: str | None,
    chart_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    options = _normalise_chart_options(chart_options)
    row_column = options["row_column"]
    _validate_chart(schema, chart_type, x_column, y_column, row_column=row_column)

    if chart_type == "bar":
        if y_column and y_column in df.columns and schema[x_column]["type"] in {"categorical", "text", "datetime"}:
            grouped = _series_from_grouped_data(df, schema, x_column, y_column, "bar", options)
            grouped = _sort_and_trim_series(grouped, options["sort_order"], options["top_n"])
            return {
                "mode": "category",
                "summary": f"Click a category to filter the preview table to rows that contributed to this bar chart.",
                "items": [
                    {
                        "id": f"bar-{index}",
                        "label": f"{x_column}: {_format_value_label(category)}",
                        "value_text": f"{y_column or 'Value'}: {_format_value_label(value, 4)}",
                        "filter_label": f"{x_column} = {_format_value_label(category)}",
                        "conditions": [
                            {
                                "type": "equals",
                                "column": x_column,
                                "value": _to_serialisable_scalar(category),
                            }
                        ],
                    }
                    for index, (category, value) in enumerate(grouped.items())
                ],
            }
        if not y_column:
            counts = _series_from_grouped_data(df, schema, x_column, None, "bar", options)
            counts = _sort_and_trim_series(counts, options["sort_order"], options["top_n"])
            return {
                "mode": "category",
                "summary": f"Click a category to filter the preview table to matching rows from this count view.",
                "items": [
                    {
                        "id": f"bar-{index}",
                        "label": f"{x_column}: {_format_value_label(category)}",
                        "value_text": f"Count: {_format_value_label(value, 0)}",
                        "filter_label": f"{x_column} = {_format_value_label(category)}",
                        "conditions": [
                            {
                                "type": "equals",
                                "column": x_column,
                                "value": _to_serialisable_scalar(category),
                            }
                        ],
                    }
                    for index, (category, value) in enumerate(counts.items())
                ],
            }
        return {
            "mode": "unsupported",
            "summary": "This bar chart uses raw numeric positions, so there is no clean category filter to apply back to the table.",
            "items": [],
        }

    if chart_type == "pie":
        series = _prepare_pie_series(df, schema, x_column, y_column, options)
        items = []
        for index, (category, value) in enumerate(series.items()):
            is_other = str(category) == "Other"
            items.append(
                {
                    "id": f"pie-{index}",
                    "label": f"{x_column}: {_format_value_label(category)}",
                    "value_text": f"{y_column or 'Value'}: {_format_value_label(value, 4)}",
                    "filter_label": f"{x_column} = {_format_value_label(category)}",
                    "conditions": [] if is_other else [{
                        "type": "equals",
                        "column": x_column,
                        "value": _to_serialisable_scalar(category),
                    }],
                    "disabled": is_other,
                }
            )
        return {
            "mode": "category",
            "summary": "Click a slice label to filter the preview table to that category. 'Other' stays informational because it combines multiple categories.",
            "items": items,
        }

    if chart_type in {"line", "area"}:
        ordered = _prepare_line_frame(df, schema, x_column, y_column, options)
        return {
            "mode": "time",
            "summary": f"Click a point label to filter the preview table to rows from that {x_column} value.",
            "items": [
                {
                    "id": f"line-{index}",
                    "label": f"{x_column}: {_format_value_label(row[x_column])}",
                    "value_text": f"{y_column}: {_format_value_label(row[y_column], 4)}",
                    "filter_label": f"{x_column} = {_format_value_label(row[x_column])}",
                    "conditions": [
                        {
                            "type": "equals",
                            "column": x_column,
                            "value": _to_serialisable_scalar(row[x_column]),
                        }
                    ],
                }
                for index, row in ordered.iterrows()
            ],
        }

    if chart_type == "heatmap":
        heatmap_table = _prepare_heatmap_table(df, schema, x_column, row_column, y_column, options)
        items: list[dict[str, Any]] = []
        for row_label, row_values in heatmap_table.iterrows():
            for column_label, value in row_values.items():
                if pd.isna(value) or float(value) == 0:
                    continue
                items.append(
                    {
                        "id": f"heatmap-{len(items)}",
                        "label": f"{_format_value_label(row_label)} x {_format_value_label(column_label)}",
                        "value_text": f"{y_column or 'Count'}: {_format_value_label(value, 4)}",
                        "filter_label": f"{row_column} = {_format_value_label(row_label)} and {x_column} = {_format_value_label(column_label)}",
                        "conditions": [
                            {"type": "equals", "column": row_column, "value": _to_serialisable_scalar(row_label)},
                            {"type": "equals", "column": x_column, "value": _to_serialisable_scalar(column_label)},
                        ],
                    }
                )
        return {
            "mode": "heatmap",
            "summary": "Click a heatmap cell label to filter the preview table to that row and column combination.",
            "items": items[:48],
        }

    if chart_type == "histogram":
        numeric = pd.to_numeric(df[x_column], errors="coerce").dropna()
        if numeric.empty:
            return {
                "mode": "unsupported",
                "summary": "This histogram has no numeric values available for preview filtering.",
                "items": [],
            }
        binned = pd.cut(numeric, bins=options["bins"], include_lowest=True, duplicates="drop")
        counts = binned.value_counts(sort=False)
        items = []
        non_empty_intervals = [(interval, count) for interval, count in counts.items() if int(count) > 0]
        for index, (interval, count) in enumerate(non_empty_intervals):
            items.append(
                {
                    "id": f"hist-{index}",
                    "label": f"{x_column}: {_format_value_label(interval.left)} to {_format_value_label(interval.right)}",
                    "value_text": f"Rows: {_format_value_label(count, 0)}",
                    "filter_label": f"{x_column} from {_format_value_label(interval.left)} to {_format_value_label(interval.right)}",
                    "conditions": [
                        {
                            "type": "range",
                            "column": x_column,
                            "min": _to_serialisable_scalar(interval.left),
                            "max": _to_serialisable_scalar(interval.right),
                            "include_max": index == len(non_empty_intervals) - 1,
                        }
                    ],
                }
            )
        return {
            "mode": "range",
            "summary": "Click a histogram range to filter the preview table to rows whose values fall inside that bin.",
            "items": items,
        }

    return {
        "mode": "unsupported",
        "summary": f"{chart_type.title()} charts do not have row-level cross-filtering yet.",
        "items": [],
    }


def _validate_chart(
    schema: dict[str, dict[str, str]],
    chart_type: str,
    x_column: str,
    y_column: str | None,
    row_column: str | None = None,
) -> None:
    if x_column not in schema:
        raise ValueError("Selected X column is not available in the transformed dataset.")
    if y_column and y_column not in schema:
        raise ValueError("Selected Y column is not available in the transformed dataset.")
    if row_column and row_column not in schema:
        raise ValueError("Selected heatmap row column is not available in the transformed dataset.")

    x_type = schema[x_column]["type"]
    x_role = schema[x_column].get("role")
    row_role = schema[row_column].get("role") if row_column and row_column in schema else None
    y_type = schema[y_column]["type"] if y_column and y_column in schema else None
    row_type = schema[row_column]["type"] if row_column and row_column in schema else None

    rules = {
        "bar": x_type in {"categorical", "datetime", "text"} or y_type == "numeric",
        "pie": x_type in {"categorical", "text"} or y_type == "numeric",
        "histogram": x_type == "numeric",
        "box": x_type == "numeric",
        "line": (x_type == "datetime" or x_role == "time") and y_type == "numeric",
        "area": (x_type == "datetime" or x_role == "time") and y_type == "numeric",
        "scatter": x_type == "numeric" and y_type == "numeric",
        "heatmap": row_column is not None
        and (row_type in {"categorical", "text", "datetime"} or row_role == "time")
        and (x_type in {"categorical", "text", "datetime"} or x_role == "time"),
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
    chart_options: dict[str, Any] | None = None,
) -> bytes:
    options = _normalise_chart_options(chart_options)
    row_column = options["row_column"]
    _validate_chart(schema, chart_type, x_column, y_column, row_column=row_column)
    primary_color, secondary_color = _palette_colors(options["palette"])

    figure, axis = plt.subplots(figsize=(10, 6))

    if chart_type == "bar":
        if y_column and y_column in df.columns and schema[x_column]["type"] in {"categorical", "text", "datetime"}:
            grouped = _series_from_grouped_data(df, schema, x_column, y_column, "bar", options)
            grouped = _sort_and_trim_series(grouped, options["sort_order"], options["top_n"])
            labels = grouped.index.astype(str).tolist()
            use_horizontal = len(labels) > 8 or _has_long_labels(labels)
            _set_categorical_figure_size(figure, labels, horizontal=use_horizontal)
            formatted_labels = [_wrap_label(label) for label in labels]
            if use_horizontal:
                sns.barplot(x=grouped.values, y=formatted_labels, ax=axis, orient="h", color=primary_color)
                axis.set_xlabel(y_column if y_column else "count")
                axis.set_ylabel(x_column)
                if options["show_value_labels"]:
                    _add_value_labels(axis, options["decimal_places"], horizontal=True)
            else:
                sns.barplot(x=formatted_labels, y=grouped.values, ax=axis, color=primary_color)
                axis.set_ylabel(y_column if y_column else "count")
                if options["show_value_labels"]:
                    _add_value_labels(axis, options["decimal_places"])
        elif y_column and y_column in df.columns:
            sns.barplot(data=df, x=x_column, y=y_column, ax=axis, errorbar=None, color=primary_color)
            if options["show_value_labels"]:
                _add_value_labels(axis, options["decimal_places"])
        else:
            counts = _series_from_grouped_data(df, schema, x_column, None, "bar", options)
            counts = _sort_and_trim_series(counts, options["sort_order"], options["top_n"])
            labels = counts.index.tolist()
            use_horizontal = len(labels) > 8 or _has_long_labels(labels)
            _set_categorical_figure_size(figure, labels, horizontal=use_horizontal)
            formatted_labels = [_wrap_label(label) for label in labels]
            if use_horizontal:
                sns.barplot(x=counts.values, y=formatted_labels, ax=axis, orient="h", color=primary_color)
                axis.set_xlabel("count")
                axis.set_ylabel(x_column)
                if options["show_value_labels"]:
                    _add_value_labels(axis, 0, horizontal=True)
            else:
                sns.barplot(x=formatted_labels, y=counts.values, ax=axis, color=primary_color)
                axis.set_ylabel("count")
                if options["show_value_labels"]:
                    _add_value_labels(axis, 0)
    elif chart_type == "pie":
        figure.set_size_inches(9, 9)
        series = _prepare_pie_series(df, schema, x_column, y_column, options)
        pie_colors = sns.color_palette("blend:" + secondary_color + "," + primary_color, n_colors=max(len(series), 2))
        axis.pie(
            series.values,
            labels=[_wrap_label(label) for label in series.index],
            autopct="%1.1f%%",
            startangle=90,
            colors=pie_colors,
        )
        axis.axis("equal")
    elif chart_type == "histogram":
        sns.histplot(data=df, x=x_column, kde=False, bins=options["bins"], ax=axis, color=primary_color)
    elif chart_type == "box":
        sns.boxplot(data=df, y=x_column, ax=axis, color=secondary_color)
        axis.set_xlabel("")
    elif chart_type == "line":
        ordered = _prepare_line_frame(df, schema, x_column, y_column, options)
        sns.lineplot(data=ordered, x=x_column, y=y_column, ax=axis, marker="o", color=primary_color)
        if options["show_value_labels"]:
            for _, row in ordered.iterrows():
                axis.text(row[x_column], row[y_column], f"{row[y_column]:.{options['decimal_places']}f}".rstrip("0").rstrip("."), fontsize=8.5, color="#42526e", ha="center", va="bottom")
    elif chart_type == "area":
        ordered = _prepare_line_frame(df, schema, x_column, y_column, options)
        x_values = ordered[x_column]
        y_values = ordered[y_column]
        axis.plot(x_values, y_values, color=primary_color, linewidth=2.2)
        axis.fill_between(x_values, y_values, color=secondary_color, alpha=0.6)
        axis.set_ylabel(y_column)
        if options["show_value_labels"]:
            for _, row in ordered.iterrows():
                axis.text(row[x_column], row[y_column], f"{row[y_column]:.{options['decimal_places']}f}".rstrip("0").rstrip("."), fontsize=8.5, color="#42526e", ha="center", va="bottom")
    elif chart_type == "scatter":
        sns.scatterplot(data=df, x=x_column, y=y_column, ax=axis, color=primary_color)
    elif chart_type == "heatmap":
        heatmap_table = _prepare_heatmap_table(df, schema, x_column, row_column, y_column, options)
        figure.set_size_inches(min(max(8.5, 4.8 + len(heatmap_table.columns) * 0.6), 18.0), min(max(5.6, 3.2 + len(heatmap_table.index) * 0.38), 15.0))
        cmap_map = {
            "blue": "Blues",
            "green": "Greens",
            "coral": "OrRd",
            "slate": "Purples",
        }
        sns.heatmap(
            heatmap_table,
            cmap=cmap_map.get(options["palette"], "Blues"),
            linewidths=0.4,
            linecolor="white",
            ax=axis,
            annot=options["show_value_labels"],
            fmt=f".{options['decimal_places']}f",
        )
        axis.set_xlabel(x_column)
        axis.set_ylabel(row_column)
    else:
        raise ValueError(f"Unsupported chart type: {chart_type}")

    axis.set_title(title or f"{chart_type.title()} chart")
    if chart_type == "bar" and axis.get_xlabel() != "count" and axis.get_ylabel() != x_column:
        axis.tick_params(axis="x", rotation=_resolve_rotation(options["label_rotation"], 25))
    elif chart_type in {"line", "area", "scatter", "heatmap"}:
        axis.tick_params(axis="x", rotation=_resolve_rotation(options["label_rotation"], 20))
    elif chart_type == "histogram":
        axis.tick_params(axis="x", rotation=_resolve_rotation(options["label_rotation"], 0))

    figure.tight_layout()
    buffer = BytesIO()
    figure.savefig(buffer, dpi=180, bbox_inches="tight", format=output_format)
    plt.close(figure)
    buffer.seek(0)
    return buffer.getvalue()
