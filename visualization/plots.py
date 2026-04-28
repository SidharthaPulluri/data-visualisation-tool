from __future__ import annotations

import math
from io import BytesIO
from textwrap import wrap
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.patheffects as path_effects
import numpy as np
import pandas as pd
import seaborn as sns

sns.set_theme(style="whitegrid")

GRAPH_ROLE_PRIORITY = {
    "count": 1.0,
    "measure": 0.96,
    "rate": 0.92,
    "time": 0.84,
    "geography": 0.8,
    "category": 0.76,
    "descriptor": 0.62,
    "identifier": 0.2,
    "constant": 0.1,
}

GRAPH_ROLE_COLORS = {
    "count": "#2f67ff",
    "measure": "#1f8f5f",
    "rate": "#dc6a4d",
    "time": "#7b61ff",
    "geography": "#0f9aa8",
    "category": "#4d77c9",
    "descriptor": "#7d8aa5",
    "identifier": "#8a94a8",
    "constant": "#a7b1c2",
}


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


def _is_numeric_like(meta: dict[str, Any]) -> bool:
    return meta.get("type") == "numeric" or meta.get("role") == "time"


def _is_categorical_like(meta: dict[str, Any]) -> bool:
    return meta.get("type") in {"categorical", "text"} or meta.get("role") in {"geography", "category", "descriptor"}


def _series_to_numeric(series: pd.Series, meta: dict[str, Any]) -> pd.Series:
    if meta.get("type") == "datetime" or meta.get("role") == "time":
        converted = pd.to_datetime(series, errors="coerce", format="mixed")
        if converted.notna().any():
            numeric = converted.astype("int64", copy=False).astype("float64")
            numeric[converted.isna()] = np.nan
            return pd.Series(numeric / 1_000_000_000.0, index=series.index)
    return pd.to_numeric(series, errors="coerce")


def _series_to_category(series: pd.Series) -> pd.Series:
    return series.astype(str).replace({"nan": np.nan, "None": np.nan, "NaT": np.nan})


def _safe_correlation(left: pd.Series, right: pd.Series) -> float:
    paired = pd.concat([left, right], axis=1).dropna()
    if len(paired) < 4:
        return 0.0
    score = paired.iloc[:, 0].corr(paired.iloc[:, 1])
    if score is None or pd.isna(score):
        return 0.0
    return float(abs(score))


def _correlation_ratio(categories: pd.Series, values: pd.Series) -> float:
    paired = pd.concat([categories, values], axis=1).dropna()
    if len(paired) < 4:
        return 0.0
    grouped = paired.groupby(paired.iloc[:, 0], dropna=False)[paired.columns[1]]
    if grouped.ngroups < 2:
        return 0.0
    overall_mean = paired.iloc[:, 1].mean()
    between = 0.0
    within = 0.0
    for _, group in grouped:
        if group.empty:
            continue
        mean = group.mean()
        between += len(group) * float((mean - overall_mean) ** 2)
        within += float(((group - mean) ** 2).sum())
    total = between + within
    if total <= 0:
        return 0.0
    return float(math.sqrt(max(between / total, 0.0)))


def _cramers_v(left: pd.Series, right: pd.Series) -> float:
    paired = pd.concat([left, right], axis=1).dropna()
    if len(paired) < 4:
        return 0.0
    contingency = pd.crosstab(paired.iloc[:, 0], paired.iloc[:, 1])
    if contingency.empty or min(contingency.shape) < 2:
        return 0.0
    observed = contingency.to_numpy(dtype="float64")
    total = observed.sum()
    if total <= 0:
        return 0.0
    row_sums = observed.sum(axis=1, keepdims=True)
    column_sums = observed.sum(axis=0, keepdims=True)
    expected = row_sums @ column_sums / total
    expected = np.where(expected == 0, np.nan, expected)
    chi_square = np.nansum((observed - expected) ** 2 / expected)
    phi_square = chi_square / total
    rows, columns = observed.shape
    denominator = min(columns - 1, rows - 1)
    if denominator <= 0:
        return 0.0
    return float(min(math.sqrt(max(phi_square / denominator, 0.0)), 1.0))


def _column_variability(series: pd.Series, meta: dict[str, Any]) -> float:
    non_null = series.dropna()
    if non_null.empty:
        return 0.0

    unique_count = max(int(non_null.nunique(dropna=True)), 1)
    if meta.get("type") == "numeric":
        numeric = _series_to_numeric(non_null, meta).dropna()
        if numeric.empty:
            return 0.0
        spread = float(numeric.std(ddof=0))
        if spread <= 0:
            return 0.15
        normalized = float(min(1.0, math.log1p(spread) / 4.0))
        return 0.35 + normalized * 0.65

    if meta.get("type") == "datetime" or meta.get("role") == "time":
        return 0.75 if unique_count > 1 else 0.2

    unique_ratio = min(unique_count / max(len(non_null), 1), 1.0)
    if unique_count <= 1:
        return 0.1
    if unique_ratio > 0.9 and meta.get("role") == "descriptor":
        return 0.25
    return float(min(1.0, 0.35 + unique_ratio * 0.9))


def _categorical_complexity_penalty(series: pd.Series, meta: dict[str, Any]) -> float:
    if not _is_categorical_like(meta):
        return 1.0
    non_null = series.dropna()
    unique_count = int(non_null.nunique(dropna=True))
    if unique_count <= 24:
        return 1.0
    return float(max(0.28, math.sqrt(24.0 / unique_count)))


def _column_importance(
    column: str,
    df: pd.DataFrame,
    schema: dict[str, dict[str, Any]],
    connectivity_score: float,
) -> float:
    meta = schema.get(column, {})
    series = df[column]
    role_priority = GRAPH_ROLE_PRIORITY.get(meta.get("role", ""), 0.6)
    completeness = float(series.notna().mean()) if len(series) else 0.0
    variability = _column_variability(series, meta)
    return float(
        0.34 * role_priority
        + 0.21 * completeness
        + 0.2 * variability
        + 0.25 * connectivity_score
    )


def _relationship_score(
    df: pd.DataFrame,
    schema: dict[str, dict[str, Any]],
    left: str,
    right: str,
) -> tuple[float, str]:
    left_meta = schema.get(left, {})
    right_meta = schema.get(right, {})

    if left_meta.get("role") in {"identifier", "constant"} or right_meta.get("role") in {"identifier", "constant"}:
        return 0.0, "ignored identifier/constant field"

    left_series = df[left]
    right_series = df[right]
    complexity_penalty = math.sqrt(
        _categorical_complexity_penalty(left_series, left_meta) * _categorical_complexity_penalty(right_series, right_meta)
    )

    if _is_numeric_like(left_meta) and _is_numeric_like(right_meta):
        score = _safe_correlation(_series_to_numeric(left_series, left_meta), _series_to_numeric(right_series, right_meta))
        return score * complexity_penalty, "numeric relationship"

    if _is_numeric_like(left_meta) and _is_categorical_like(right_meta):
        score = _correlation_ratio(_series_to_category(right_series), _series_to_numeric(left_series, left_meta))
        return score * complexity_penalty, "group separation"

    if _is_categorical_like(left_meta) and _is_numeric_like(right_meta):
        score = _correlation_ratio(_series_to_category(left_series), _series_to_numeric(right_series, right_meta))
        return score * complexity_penalty, "group separation"

    if _is_categorical_like(left_meta) and _is_categorical_like(right_meta):
        score = _cramers_v(_series_to_category(left_series), _series_to_category(right_series))
        return score * complexity_penalty, "categorical association"

    if left_meta.get("type") == "datetime" or right_meta.get("type") == "datetime":
        score = _safe_correlation(_series_to_numeric(left_series, left_meta), _series_to_numeric(right_series, right_meta))
        return score * complexity_penalty, "timeline relationship"

    return 0.0, "no stable relationship"


def _normalize_scores(scores: dict[str, float]) -> dict[str, float]:
    if not scores:
        return {}
    values = list(scores.values())
    minimum = min(values)
    maximum = max(values)
    if math.isclose(minimum, maximum):
        return {key: 0.7 for key in scores}
    return {key: (value - minimum) / (maximum - minimum) for key, value in scores.items()}


def _build_feature_graph(
    df: pd.DataFrame,
    schema: dict[str, dict[str, Any]],
    focus_column: str,
) -> dict[str, Any]:
    candidate_columns = [
        column
        for column, meta in schema.items()
        if meta.get("role") != "constant"
    ]
    if focus_column in schema and focus_column not in candidate_columns:
        candidate_columns.insert(0, focus_column)
    candidate_columns = candidate_columns[: max(len(candidate_columns), 1)]

    if len(candidate_columns) < 2:
        raise ValueError("Feature relationship graph needs at least two usable columns.")

    edges: list[dict[str, Any]] = []
    connectivity = {column: 0.0 for column in candidate_columns}
    strongest_by_type: list[tuple[float, str, str, str]] = []
    threshold = 0.18 if len(candidate_columns) <= 18 else 0.24

    for index, left in enumerate(candidate_columns):
        for right in candidate_columns[index + 1 :]:
            score, relation_kind = _relationship_score(df, schema, left, right)
            if score < threshold:
                continue
            weighted_score = score
            if schema[left].get("role") == "descriptor" or schema[right].get("role") == "descriptor":
                weighted_score *= 0.88
            edges.append(
                {
                    "source": left,
                    "target": right,
                    "weight": float(weighted_score),
                    "kind": relation_kind,
                }
            )
            connectivity[left] += float(weighted_score)
            connectivity[right] += float(weighted_score)
            strongest_by_type.append((float(weighted_score), left, right, relation_kind))

    if not edges:
        raise ValueError(
            "This dataset does not have strong enough column relationships for a feature graph yet. "
            "Try a view with more varied numeric or categorical fields."
        )

    normalized_connectivity = _normalize_scores(connectivity)
    node_importance = {
        column: _column_importance(column, df, schema, normalized_connectivity.get(column, 0.0))
        for column in candidate_columns
    }
    normalized_importance = _normalize_scores(node_importance)

    return {
        "columns": candidate_columns,
        "edges": edges,
        "connectivity": connectivity,
        "normalized_connectivity": normalized_connectivity,
        "importance": node_importance,
        "normalized_importance": normalized_importance,
        "top_relationships": sorted(strongest_by_type, reverse=True)[:6],
        "focus_column": focus_column,
    }


def _force_layout(feature_graph: dict[str, Any]) -> dict[str, tuple[float, float]]:
    columns = feature_graph["columns"]
    edges = feature_graph["edges"]
    focus_column = feature_graph["focus_column"]
    count = len(columns)
    angle_step = (2 * math.pi) / max(count, 1)
    radius = 2.4 + count * 0.08
    positions = np.array(
        [
            [math.cos(index * angle_step) * radius, math.sin(index * angle_step) * radius]
            for index in range(count)
        ],
        dtype="float64",
    )
    index_map = {column: index for index, column in enumerate(columns)}
    focus_index = index_map.get(focus_column, 0)
    positions[focus_index] *= 0.45
    weights = np.zeros((count, count), dtype="float64")
    for edge in edges:
        left = index_map[edge["source"]]
        right = index_map[edge["target"]]
        weights[left, right] = edge["weight"]
        weights[right, left] = edge["weight"]

    area = max(radius * radius * math.pi, 1.0)
    optimal_distance = math.sqrt(area / count)
    temperature = radius * 0.16

    for _ in range(140):
        delta = positions[:, np.newaxis, :] - positions[np.newaxis, :, :]
        distance = np.linalg.norm(delta, axis=2)
        distance = np.where(distance < 0.08, 0.08, distance)

        repulsion = (optimal_distance**2 / distance**2)[:, :, np.newaxis] * delta
        displacement = np.nansum(repulsion, axis=1)

        for left in range(count):
            for right in range(left + 1, count):
                weight = weights[left, right]
                if weight <= 0:
                    continue
                delta_vector = positions[left] - positions[right]
                edge_distance = max(float(np.linalg.norm(delta_vector)), 0.08)
                attraction = (edge_distance**2 / optimal_distance) * weight * 0.055
                direction = delta_vector / edge_distance
                displacement[left] -= direction * attraction
                displacement[right] += direction * attraction

        displacement -= positions * 0.018
        displacement[focus_index] -= positions[focus_index] * 0.11

        displacement_norm = np.linalg.norm(displacement, axis=1)
        displacement_norm = np.where(displacement_norm < 0.02, 0.02, displacement_norm)
        step = np.minimum(displacement_norm, temperature)[:, np.newaxis]
        positions += displacement / displacement_norm[:, np.newaxis] * step
        temperature *= 0.963

    positions -= positions.mean(axis=0)
    max_extent = np.max(np.abs(positions))
    if max_extent > 0:
        positions /= max_extent
    positions *= 4.3
    positions[focus_index] *= 0.15
    return {column: (float(positions[index, 0]), float(positions[index, 1])) for index, column in enumerate(columns)}


def _draw_feature_graph(
    axis: plt.Axes,
    df: pd.DataFrame,
    schema: dict[str, dict[str, Any]],
    focus_column: str,
    title: str | None,
    palette: str,
) -> dict[str, Any]:
    feature_graph = _build_feature_graph(df, schema, focus_column)
    positions = _force_layout(feature_graph)
    axis.set_facecolor("#f8fbff")
    axis.axis("off")

    palette_primary, palette_secondary = _palette_colors(palette)
    strongest_weight = max(edge["weight"] for edge in feature_graph["edges"])
    for edge in sorted(feature_graph["edges"], key=lambda item: item["weight"]):
        source_x, source_y = positions[edge["source"]]
        target_x, target_y = positions[edge["target"]]
        normalized = edge["weight"] / strongest_weight if strongest_weight else 0.0
        axis.plot(
            [source_x, target_x],
            [source_y, target_y],
            color=palette_primary,
            alpha=0.14 + normalized * 0.35,
            linewidth=0.6 + normalized * 1.8,
            zorder=1,
        )

    for column in feature_graph["columns"]:
        meta = schema.get(column, {})
        importance = feature_graph["normalized_importance"].get(column, 0.5)
        role_color = GRAPH_ROLE_COLORS.get(meta.get("role", ""), GRAPH_ROLE_COLORS["descriptor"])
        x, y = positions[column]
        node_size = 520 + importance * 1850
        axis.scatter(x, y, s=node_size * 1.55, color=palette_secondary, alpha=0.14, linewidths=0, zorder=2)
        axis.scatter(x, y, s=node_size, color=role_color, edgecolors="white", linewidths=1.8, alpha=0.94, zorder=3)
        if column == feature_graph["focus_column"]:
            axis.scatter(x, y, s=node_size * 1.35, facecolors="none", edgecolors=palette_primary, linewidths=2.0, zorder=4)

        label = _wrap_label(column, width=16)
        text = axis.text(
            x,
            y,
            label,
            ha="center",
            va="center",
            fontsize=8.4 + importance * 1.8,
            color="#10233d",
            weight="semibold" if importance > 0.55 else "medium",
            zorder=5,
        )
        text.set_path_effects([path_effects.withStroke(linewidth=2.2, foreground="white", alpha=0.95)])

    top_lines = []
    for weight, left, right, relation_kind in feature_graph["top_relationships"][:4]:
        top_lines.append(f"{left} <> {right} ({relation_kind}, {weight:.2f})")

    axis.text(
        0.02,
        0.02,
        "Stronger links pull features closer. Bigger nodes carry more structural importance in this dataset.\n"
        + ("Top links: " + " | ".join(top_lines) if top_lines else ""),
        transform=axis.transAxes,
        fontsize=8.7,
        color="#42526e",
        va="bottom",
        bbox={"boxstyle": "round,pad=0.5", "facecolor": "white", "edgecolor": "#d7e4f2", "alpha": 0.96},
    )
    axis.set_title(title or "Feature relationship graph")
    axis.set_xlim(-5.45, 5.45)
    axis.set_ylim(-5.1, 5.1)
    return feature_graph


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

    if chart_type == "feature_graph":
        feature_graph = _build_feature_graph(df, schema, x_column)
        top_links = [
            f"{left} <> {right} ({relation_kind}, {weight:.2f})"
            for weight, left, right, relation_kind in feature_graph["top_relationships"][:5]
        ]
        summary = (
            f"This feature graph compares columns, not rows. {x_column} is the focus feature, "
            "and stronger dependencies pull nodes closer together."
        )
        if top_links:
            summary += " Strongest links: " + "; ".join(top_links) + "."
        return {
            "mode": "unsupported",
            "summary": summary,
            "items": [],
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
        "feature_graph": x_column in schema and len([name for name, meta in schema.items() if meta.get("role") != "constant"]) >= 2,
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
    elif chart_type == "feature_graph":
        figure.set_size_inches(11.6, 8.2)
        _draw_feature_graph(axis, df, schema, x_column, title, options["palette"])
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
