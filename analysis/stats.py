from __future__ import annotations

import re
from typing import Any

import pandas as pd


def _tokenize(text: str) -> list[str]:
    return [token for token in re.split(r"[_\W]+", str(text).lower()) if token]


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
    tokens = _tokenize(normalized)
    unique_ratio = series.nunique(dropna=True) / max(len(series), 1)
    return "id" in tokens and unique_ratio >= 0.9


def _column_matches(column_name: str, terms: set[str]) -> bool:
    tokens = set(_tokenize(column_name))
    return bool(tokens.intersection(terms))


def _format_label(label: str) -> str:
    text = str(label)
    text = re.sub(r"Col\d+(?:Col\d+)*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"__col_\d+\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"_col[\d_]+", "", text, flags=re.IGNORECASE)
    text = text.replace("StateUT", "State/UT")
    text = text.replace("Invsgnat", "Investigation at")
    text = text.replace("Invsgn", "Investigation")
    text = text.replace("_", " ")
    text = re.sub(r"\b\d+\b\s*$", "", text)
    text = re.sub(r"\s+", " ", text).strip(" -")
    return text


def _format_dimension_value(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return text
    if text.islower() or text.isupper():
        return text.title()
    return text


def _is_generic_generated_name(column_name: str) -> bool:
    return bool(re.fullmatch(r"(column|feature|vote)_\d+", str(column_name).strip().lower()))


def _metric_slot_label(slot: str) -> str:
    labels = {
        "primary_count": "Primary volume field",
        "pending_count": "Backlog field",
        "disposed_count": "Disposition field",
        "primary_rate": "Primary rate field",
        "pending_rate": "Backlog rate field",
    }
    return labels.get(slot, slot.replace("_", " ").title())


def _headline_metric_label(slot: str, column: str) -> str:
    tokens = set(_tokenize(column))
    pretty = _format_label(column)

    if slot == "primary_count":
        if {"reported", "registered"}.intersection(tokens):
            return "Reported cases"
        if {"incident", "incidence"}.intersection(tokens):
            return pretty
        if {"deaths", "mortality"}.intersection(tokens):
            return pretty
        if {"prevalence"}.intersection(tokens):
            return pretty
        return "Primary volume"

    if slot == "disposed_count":
        if "disposed" in tokens:
            return "Disposed cases"
        if {"chargesheeted", "charge"}.intersection(tokens):
            return "Chargesheeted cases"
        return "Disposition field"

    if slot == "pending_count":
        if {"pending", "pendency", "backlog"}.intersection(tokens):
            return "Pending at year end"
        return "Backlog field"

    if slot == "primary_rate":
        if "chargesheeting" in tokens:
            return "Chargesheeting rate"
        if "detection" in tokens:
            return "Detection rate"
        return pretty

    if slot == "pending_rate":
        if {"pending", "pendency", "backlog"}.intersection(tokens):
            return "Pendency percentage"
        return pretty

    return pretty


def _metric_choice_map(metric_choices: list[dict[str, Any]]) -> dict[str, str]:
    return {
        item["slot"]: item["column"]
        for item in metric_choices
        if item.get("slot") and item.get("column")
    }


def _format_value(value: Any, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    numeric = float(value)
    if numeric.is_integer():
        return f"{int(numeric):,}"
    return f"{numeric:,.{digits}f}".rstrip("0").rstrip(".")


def _to_serializable_number(value: Any, digits: int = 4) -> float | int | None:
    if value is None or pd.isna(value):
        return None
    numeric = float(value)
    if numeric.is_integer():
        return int(numeric)
    return round(numeric, digits)


def _numeric_like_ratio(series: pd.Series) -> float:
    sample = series.dropna().astype(str).str.strip()
    if sample.empty:
        return 0.0
    numeric_like = sample.str.fullmatch(r"[-+]?\d+(\.\d+)?").fillna(False)
    return float(numeric_like.mean())


def _pick_dimension_column(schema: dict[str, dict[str, Any]], df: pd.DataFrame) -> str | None:
    generic_categorical_columns: list[str] = []
    candidates: list[tuple[int, int, str]] = []
    for column, meta in schema.items():
        if meta["type"] not in {"categorical", "text", "datetime"}:
            continue
        if meta.get("role") == "identifier":
            continue
        unique_count = int(meta.get("unique", 0))
        if unique_count <= 1:
            continue
        if _numeric_like_ratio(df[column]) >= 0.75:
            continue

        name_tokens = set(_tokenize(column))
        if {"sl", "no", "col"}.issubset(name_tokens) or {"serial", "number"}.issubset(name_tokens):
            continue

        if _is_generic_generated_name(column):
            generic_categorical_columns.append(column)

        semantic_score = {
            "geography": 1,
            "category": 3,
            "time": 4,
            "descriptor": 5,
        }.get(meta.get("role"), 6)
        if column in {"party", "class_label", "label"}:
            semantic_score = 0
        if _column_matches(column, {"country", "territory", "state", "district", "province", "city", "ut"}):
            semantic_score = 0
        elif _column_matches(column, {"region", "continent", "area"}):
            semantic_score = 2

        code_penalty = 1 if _column_matches(column, {"iso", "code", "numeric"}) else 0
        richness_score = -unique_count if meta.get("role") in {"geography", "category"} else abs(unique_count - min(max(len(df) // 4, 4), 40))
        candidates.append((semantic_score, code_penalty, richness_score, column))

    if generic_categorical_columns and len(generic_categorical_columns) == len(candidates):
        ranked_generic = sorted(
            generic_categorical_columns,
            key=lambda name: (
                0 if name in {"party", "class_label", "label"} else 1,
                int(schema[name].get("unique", 0) > 12),
                int(schema[name].get("unique", 0)),
                list(df.columns).index(name),
            ),
        )
        return ranked_generic[0]

    candidates.sort()
    return candidates[0][3] if candidates else None


def _split_detail_and_total_rows(df: pd.DataFrame, dimension_column: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    labels = df[dimension_column].astype(str)
    total_mask = labels.str.fullmatch(r".*\btotal\b.*", case=False, na=False)
    return df.loc[~total_mask].copy(), df.loc[total_mask].copy()


def _pick_overall_total_row(total_df: pd.DataFrame, dimension_column: str) -> pd.Series | None:
    if total_df.empty:
        return None

    labels = total_df[dimension_column].astype(str).str.lower()
    priority_terms = (
        "all india",
        "overall",
        "grand total",
        "total all",
        "all states",
        "all districts",
    )
    for term in priority_terms:
        matches = total_df[labels.str.contains(term, na=False)]
        if not matches.empty:
            return matches.iloc[0]

    return total_df.iloc[-1]


def _score_metric_candidates(
    columns: list[str],
    schema: dict[str, dict[str, Any]],
    *,
    include_terms: set[str] | None = None,
    weighted_terms: dict[str, int] | None = None,
    exclude_terms: set[str] | None = None,
    required_terms: set[str] | None = None,
) -> list[dict[str, Any]]:
    include_terms = include_terms or set()
    weighted_terms = weighted_terms or {}
    exclude_terms = exclude_terms or set()
    required_terms = required_terms or set()

    ranked: list[dict[str, Any]] = []
    for column in columns:
        tokens = set(_tokenize(column))
        if exclude_terms.intersection(tokens):
            continue
        if required_terms and not required_terms.intersection(tokens):
            continue

        matched = len(tokens.intersection(include_terms))
        weighted_hits = {term: weight for term, weight in weighted_terms.items() if term in tokens}
        weighted_score = sum(weighted_hits.values())
        if not matched and not weighted_score:
            continue

        role = schema.get(column, {}).get("role")
        role_bonus = {
            "count": 3,
            "rate": 3,
            "measure": 2,
        }.get(role, 0)
        completeness_bonus = int((schema.get(column, {}).get("completeness", 0) or 0) * 2)
        warning_penalty = len(schema.get(column, {}).get("warnings", []))
        score = weighted_score * 10 + matched * 2 + role_bonus + completeness_bonus - warning_penalty

        reasons: list[str] = []
        if weighted_hits:
            reasons.append(
                "matched "
                + ", ".join(f"{term} (+{weight})" for term, weight in sorted(weighted_hits.items()))
            )
        if matched:
            reasons.append(f"matched {matched} supporting term{'s' if matched != 1 else ''}")
        if role in {"count", "rate", "measure"}:
            reasons.append(f"role: {role}")
        if schema.get(column, {}).get("warnings"):
            reasons.append("has caution flags")

        ranked.append(
            {
                "column": column,
                "score": score,
                "role": role,
                "reasons": reasons,
            }
        )

    ranked.sort(key=lambda item: (-item["score"], len(item["column"]), item["column"]))
    return ranked


def _pick_metric_column(
    columns: list[str],
    schema: dict[str, dict[str, Any]],
    *,
    include_terms: set[str] | None = None,
    weighted_terms: dict[str, int] | None = None,
    exclude_terms: set[str] | None = None,
    required_terms: set[str] | None = None,
) -> str | None:
    ranked = _score_metric_candidates(
        columns,
        schema,
        include_terms=include_terms,
        weighted_terms=weighted_terms,
        exclude_terms=exclude_terms,
        required_terms=required_terms,
    )
    return ranked[0]["column"] if ranked else None


def _detect_dataset_intent(
    df: pd.DataFrame,
    schema: dict[str, dict[str, Any]],
    *,
    focus_dimension: str | None,
    detail_df: pd.DataFrame,
    total_df: pd.DataFrame,
) -> dict[str, Any]:
    role_counts: dict[str, int] = {}
    for meta in schema.values():
        role = meta.get("role") or "unknown"
        role_counts[role] = role_counts.get(role, 0) + 1

    time_fields = [
        name
        for name, meta in schema.items()
        if meta["type"] == "datetime" or meta.get("role") == "time"
    ]
    category_fields = [
        name
        for name, meta in schema.items()
        if meta["type"] in {"categorical", "text"} and meta.get("role") in {"category", "geography"}
    ]
    numeric_fields = [name for name, meta in schema.items() if meta["type"] == "numeric" and meta.get("role") != "constant"]

    intent_key = "structured_table"
    intent_label = "Structured comparison table"
    confidence = 0.52
    reasons: list[str] = []

    if total_df.shape[0] >= 1 and focus_dimension and len(numeric_fields) >= 2:
        intent_key = "aggregated_summary_table"
        intent_label = "Aggregated comparison table"
        confidence = 0.86
        reasons.append("It includes summary rows such as totals, which suggests the file is already aggregated.")
        reasons.append(f"The main comparison field looks like {_format_label(focus_dimension)} rather than row-level transactions.")
    elif time_fields and category_fields and len(numeric_fields) >= 2:
        intent_key = "panel_comparison"
        intent_label = "Panel comparison dataset"
        confidence = 0.84
        reasons.append(
            f"It has both a time field ({_format_label(time_fields[0])}) and a comparison field ({_format_label(category_fields[0])})."
        )
        reasons.append("That usually means the same groups are being tracked across time.")
    elif time_fields and len(numeric_fields) >= 1 and (focus_dimension in time_fields or not category_fields):
        intent_key = "time_series_table"
        intent_label = "Time-series summary table"
        confidence = 0.78
        reasons.append(f"It is organized around {_format_label(time_fields[0])} with numeric measures to track over time.")
    elif len(category_fields) >= 2 and len(numeric_fields) >= 1:
        intent_key = "cross_tab_table"
        intent_label = "Cross-tab or matrix-style table"
        confidence = 0.7
        reasons.append("It contains multiple category-style fields plus numeric measures, which is typical of a comparison matrix.")
    elif len(category_fields) >= 3 and not numeric_fields:
        intent_key = "categorical_response_table"
        intent_label = "Categorical response dataset"
        confidence = 0.74
        reasons.append("Most fields are categorical responses rather than numeric measures.")
        reasons.append("That usually means the file is meant for counts, splits, and response-pattern comparisons.")
    else:
        reasons.append("It has a stable comparison field and numeric measures, so it can still be analyzed as a structured table.")

    if detail_df.shape[0] <= 20:
        reasons.append("The table is fairly compact, so rankings and direct comparisons should be easy to read.")
    elif detail_df.shape[0] >= 200:
        reasons.append("The table has many detail rows, so top-N filtering may be useful before charting.")

    return {
        "key": intent_key,
        "label": intent_label,
        "confidence": round(confidence, 2),
        "reasons": reasons,
        "role_counts": role_counts,
    }


def _top_rankings(
    df: pd.DataFrame,
    *,
    dimension_column: str,
    metric_column: str,
    ascending: bool,
    limit: int = 5,
    secondary_column: str | None = None,
) -> list[dict[str, Any]]:
    selected_columns = [dimension_column, metric_column]
    if secondary_column and secondary_column not in selected_columns:
        selected_columns.append(secondary_column)
    usable = df[selected_columns].copy()
    usable[metric_column] = pd.to_numeric(usable[metric_column], errors="coerce")
    usable = usable.dropna(subset=[metric_column])
    usable = usable.sort_values(metric_column, ascending=ascending).head(limit)

    rows: list[dict[str, Any]] = []
    for _, row in usable.iterrows():
        item = {
            "label": _format_dimension_value(row[dimension_column]),
            "value": _to_serializable_number(row[metric_column]),
            "formatted_value": _format_value(row[metric_column]),
        }
        if secondary_column:
            item["secondary"] = _to_serializable_number(row[secondary_column])
            item["formatted_secondary"] = _format_value(row[secondary_column])
        rows.append(item)
    return rows


def _find_supporting_metric(
    numeric_columns: list[str],
    schema: dict[str, dict[str, Any]],
    *,
    weighted_terms: dict[str, int],
    exclude_terms: set[str] | None = None,
    required_terms: set[str] | None = None,
) -> str | None:
    ranked = _score_metric_candidates(
        numeric_columns,
        schema,
        weighted_terms=weighted_terms,
        exclude_terms=exclude_terms,
        required_terms=required_terms,
    )
    return ranked[0]["column"] if ranked else None


def _build_consistency_checks(
    *,
    detail_df: pd.DataFrame,
    overall_row: pd.Series | None,
    metric_choices: list[dict[str, Any]],
    numeric_columns: list[str],
    schema: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    if overall_row is None:
        return checks

    choice_map = _metric_choice_map(metric_choices)

    for slot in ("primary_count", "disposed_count", "pending_count"):
        column = choice_map.get(slot)
        if not column or column not in detail_df.columns or column not in overall_row.index:
            continue

        detail_total = pd.to_numeric(detail_df[column], errors="coerce").sum()
        overall_value = pd.to_numeric(pd.Series([overall_row[column]]), errors="coerce").iloc[0]
        if pd.isna(overall_value):
            continue

        difference = float(detail_total - overall_value)
        passed = abs(difference) < 0.5
        checks.append(
            {
                "title": f"{_metric_slot_label(slot)} total check",
                "status": "ok" if passed else "warning",
                "message": (
                    f"Detailed rows add up cleanly to the summary total for {_format_label(column)}."
                    if passed
                    else f"Detailed rows sum to {_format_value(detail_total)} but the summary row shows {_format_value(overall_value)} for {_format_label(column)}."
                ),
            }
        )

    chargesheeted_count = _find_supporting_metric(
        numeric_columns,
        schema,
        weighted_terms={"submitted": 4, "chargesheeted": 5, "charge": 4},
        exclude_terms={"rate", "percent", "percentage", "disposed", "prev", "during", "year"},
    )
    disposed_count = choice_map.get("disposed_count")
    primary_rate = choice_map.get("primary_rate")
    if chargesheeted_count and disposed_count and primary_rate:
        numerator = pd.to_numeric(pd.Series([overall_row.get(chargesheeted_count)]), errors="coerce").iloc[0]
        denominator = pd.to_numeric(pd.Series([overall_row.get(disposed_count)]), errors="coerce").iloc[0]
        reported_rate = pd.to_numeric(pd.Series([overall_row.get(primary_rate)]), errors="coerce").iloc[0]
        if pd.notna(numerator) and pd.notna(denominator) and pd.notna(reported_rate) and denominator:
            calculated_rate = float(numerator) / float(denominator) * 100
            difference = abs(calculated_rate - float(reported_rate))
            checks.append(
                {
                    "title": "Primary rate formula check",
                    "status": "ok" if difference < 0.25 else "warning",
                    "message": (
                        f"{_format_label(primary_rate)} is consistent with {_format_label(chargesheeted_count)} divided by {_format_label(disposed_count)}."
                        if difference < 0.25
                        else f"{_format_label(primary_rate)} looks off. The summary row reports {_format_value(reported_rate)} but the available counts imply about {_format_value(calculated_rate)}."
                    ),
                }
            )

    total_cases = _find_supporting_metric(
        numeric_columns,
        schema,
        weighted_terms={"total": 4, "investigation": 4, "cases": 1},
        exclude_terms={"pending", "pendency", "rate", "percent", "percentage", "disposed", "chargesheeted", "charge"},
    )
    pending_count = choice_map.get("pending_count")
    pending_rate = choice_map.get("pending_rate")
    if total_cases and pending_count and pending_rate:
        numerator = pd.to_numeric(pd.Series([overall_row.get(pending_count)]), errors="coerce").iloc[0]
        denominator = pd.to_numeric(pd.Series([overall_row.get(total_cases)]), errors="coerce").iloc[0]
        reported_rate = pd.to_numeric(pd.Series([overall_row.get(pending_rate)]), errors="coerce").iloc[0]
        if pd.notna(numerator) and pd.notna(denominator) and pd.notna(reported_rate) and denominator:
            calculated_rate = float(numerator) / float(denominator) * 100
            difference = abs(calculated_rate - float(reported_rate))
            checks.append(
                {
                    "title": "Backlog rate formula check",
                    "status": "ok" if difference < 0.25 else "warning",
                    "message": (
                        f"{_format_label(pending_rate)} is consistent with {_format_label(pending_count)} over {_format_label(total_cases)}."
                        if difference < 0.25
                        else f"{_format_label(pending_rate)} looks off. The summary row reports {_format_value(reported_rate)} but the available counts imply about {_format_value(calculated_rate)}."
                    ),
                }
            )

    return checks


def _build_anomaly_flags(
    *,
    detail_df: pd.DataFrame,
    dimension_column: str,
    metric_choices: list[dict[str, Any]],
    numeric_columns: list[str],
    schema: dict[str, dict[str, Any]],
) -> list[str]:
    flags: list[str] = []
    if detail_df.empty:
        return flags

    choice_map = _metric_choice_map(metric_choices)

    def add_dominance_flag(slot: str, label: str) -> None:
        column = choice_map.get(slot)
        if not column:
            return
        series = pd.to_numeric(detail_df[column], errors="coerce").dropna()
        if series.empty or series.sum() <= 0:
            return
        top_idx = series.idxmax()
        top_value = float(series.loc[top_idx])
        share = top_value / float(series.sum()) * 100
        if share >= 50:
            flags.append(
                f"{_format_dimension_value(detail_df.loc[top_idx, dimension_column])} contributes about {share:.1f}% of all {label}, so national totals are heavily concentrated."
            )
        elif share >= 35:
            flags.append(
                f"{_format_dimension_value(detail_df.loc[top_idx, dimension_column])} is unusually dominant in {label}, contributing about {share:.1f}% of the total."
            )

        sorted_values = series.sort_values(ascending=False)
        if len(sorted_values) >= 2 and sorted_values.iloc[1] > 0:
            ratio = float(sorted_values.iloc[0] / sorted_values.iloc[1])
            if ratio >= 4:
                flags.append(
                    f"The top {label} value is about {ratio:.1f}x the second-highest value, which suggests an extreme concentration pattern."
                )

    add_dominance_flag("primary_count", _format_label(choice_map.get("primary_count", "primary count")))
    add_dominance_flag("pending_count", _format_label(choice_map.get("pending_count", "pending count")))

    primary_rate = choice_map.get("primary_rate")
    disposed_count = choice_map.get("disposed_count")
    if primary_rate:
        rate_series = pd.to_numeric(detail_df[primary_rate], errors="coerce")
        volume_series = pd.to_numeric(detail_df[disposed_count], errors="coerce") if disposed_count else None
        extreme_mask = rate_series.isin([0, 100])
        if volume_series is not None:
            tiny_mask = volume_series.fillna(0) <= 10
            flagged = detail_df[extreme_mask & tiny_mask]
            if not flagged.empty:
                sample = ", ".join(_format_dimension_value(value) for value in flagged[dimension_column].head(3))
                flags.append(
                    f"Some extreme {_format_label(primary_rate)} values come from very small case volumes, especially {sample}. Read those percentages cautiously."
                )

    pending_rate = choice_map.get("pending_rate")
    total_cases = _find_supporting_metric(
        numeric_columns,
        schema,
        weighted_terms={"total": 4, "investigation": 4, "cases": 1},
        exclude_terms={"pending", "pendency", "rate", "percent", "percentage", "disposed", "chargesheeted", "charge"},
    )
    if pending_rate and total_cases:
        rate_series = pd.to_numeric(detail_df[pending_rate], errors="coerce")
        total_series = pd.to_numeric(detail_df[total_cases], errors="coerce")
        flagged = detail_df[(rate_series >= 90) & (total_series <= 10)]
        if not flagged.empty:
            sample = ", ".join(_format_dimension_value(value) for value in flagged[dimension_column].head(3))
            flags.append(
                f"Several near-total backlog rates come from tiny totals, including {sample}. Those percentages are real, but the volumes are very small."
            )

    return flags[:6]


def _build_dataset_story(
    df: pd.DataFrame,
    schema: dict[str, dict[str, Any]],
    numeric_columns: list[str],
) -> dict[str, Any]:
    dimension_column = _pick_dimension_column(schema, df)
    if not dimension_column:
        return {
            "focus_dimension": None,
            "overview": [],
            "headline_metrics": [],
            "ranking_sections": [],
            "takeaways": [],
            "detail_row_count": int(len(df)),
            "summary_row_count": 0,
        }

    detail_df, total_df = _split_detail_and_total_rows(df, dimension_column)
    if detail_df.empty:
        detail_df = df.copy()
    overall_row = _pick_overall_total_row(total_df, dimension_column)
    dataset_intent = _detect_dataset_intent(
        df,
        schema,
        focus_dimension=dimension_column,
        detail_df=detail_df,
        total_df=total_df,
    )
    compare_df = detail_df.copy()
    if detail_df[dimension_column].astype(str).duplicated().any():
        aggregation_map: dict[str, str] = {}
        for column in numeric_columns:
            role = schema.get(column, {}).get("role")
            aggregation_map[column] = "mean" if role == "rate" else "sum"
        if aggregation_map:
            compare_df = (
                detail_df.groupby(dimension_column, dropna=False)[list(aggregation_map.keys())]
                .agg(aggregation_map)
                .reset_index()
            )
        else:
            compare_df = (
                detail_df.groupby(dimension_column, dropna=False)
                .size()
                .reset_index(name="row_count")
            )

    metric_choices: list[dict[str, Any]] = []

    def choose_metric(slot: str, **kwargs: Any) -> str | None:
        ranked = _score_metric_candidates(numeric_columns, schema, **kwargs)
        if ranked:
            metric_choices.append(
                {
                    "slot": slot,
                    "slot_label": _metric_slot_label(slot),
                    "column": ranked[0]["column"],
                    "formatted_column": _format_label(ranked[0]["column"]),
                    "score": ranked[0]["score"],
                    "reasons": ranked[0]["reasons"],
                    "alternatives": [
                        {
                            "column": item["column"],
                            "score": item["score"],
                        }
                        for item in ranked[1:4]
                    ],
                }
            )
            return ranked[0]["column"]
        return None

    primary_count = choose_metric(
        "primary_count",
        weighted_terms={"reported": 5, "registered": 5, "records": 4, "cases": 1, "count": 1},
        exclude_terms={"pending", "pendency", "rate", "percent", "percentage", "reopened", "quashed", "stayed", "withdrawn", "transferred", "final", "charge", "chargesheeted", "disposed"},
    ) or choose_metric(
        "primary_count",
        weighted_terms={"total": 4, "investigation": 4, "cases": 1},
        exclude_terms={"pending", "pendency", "rate", "percent", "percentage", "final", "charge", "chargesheeted", "disposed"},
    ) or (numeric_columns[0] if numeric_columns else None)
    pending_count = choose_metric(
        "pending_count",
        weighted_terms={"pending": 5, "pendency": 5, "backlog": 4, "end": 2},
        exclude_terms={"rate", "percent", "percentage"},
    )
    disposed_count = choose_metric(
        "disposed_count",
        weighted_terms={"disposed": 8, "police": 2, "closed": 3},
        exclude_terms={"rate", "percent", "percentage", "chargesheeted", "charge"},
    ) or choose_metric(
        "disposed_count",
        weighted_terms={"chargesheeted": 5, "charge": 4, "disposed": 2},
        exclude_terms={"rate", "percent", "percentage"},
    )
    primary_rate = choose_metric(
        "primary_rate",
        weighted_terms={"chargesheeting": 5, "chargesheeted": 5, "charge": 4, "clearance": 4, "rate": 1},
        exclude_terms={"pending", "pendency"},
        required_terms={"rate", "percent", "percentage"},
    )
    pending_rate = choose_metric(
        "pending_rate",
        weighted_terms={"pending": 5, "pendency": 5, "backlog": 4},
        required_terms={"pending", "pendency", "backlog"},
    )

    headline_metrics: list[dict[str, Any]] = []
    slot_map = {
        "primary_count": primary_count,
        "disposed_count": disposed_count,
        "pending_count": pending_count,
        "primary_rate": primary_rate,
        "pending_rate": pending_rate,
    }

    for slot, column in slot_map.items():
        if not column:
            continue
        if overall_row is not None and column in overall_row.index:
            source = overall_row[column]
        else:
            role = schema.get(column, {}).get("role")
            series = pd.to_numeric(detail_df[column], errors="coerce")
            source = series.mean() if role == "rate" else series.sum()
        headline_metrics.append(
            {
                "label": _headline_metric_label(slot, column),
                "column": column,
                "value": _to_serializable_number(source),
                "formatted_value": _format_value(source),
            }
        )

    if not headline_metrics and numeric_columns == []:
        headline_metrics.append(
            {
                "label": "Records",
                "column": None,
                "value": int(len(detail_df)),
                "formatted_value": _format_value(len(detail_df)),
            }
        )
        headline_metrics.append(
            {
                "label": "Response fields",
                "column": None,
                "value": max(int(df.shape[1]) - 1, 0),
                "formatted_value": _format_value(max(int(df.shape[1]) - 1, 0)),
            }
        )

    overview = [
        f"Dataset type: {dataset_intent['label']}.",
        f"Main comparison field: {_format_label(dimension_column)}.",
        f"Detailed rows available: {len(detail_df)}."
        + (f" Summary rows detected: {len(total_df)}." if len(total_df) else ""),
    ]

    ranking_sections: list[dict[str, Any]] = []
    if primary_count:
        ranking_sections.append(
            {
                "title": f"Top {_format_label(dimension_column)} by {_format_label(primary_count)}",
                "metric": primary_count,
                "items": _top_rankings(
                    compare_df,
                    dimension_column=dimension_column,
                    metric_column=primary_count,
                    ascending=False,
                ),
            }
        )
    elif "row_count" in compare_df.columns:
        ranking_sections.append(
            {
                "title": f"Most common {_format_label(dimension_column)} values",
                "metric": "row_count",
                "items": _top_rankings(
                    compare_df,
                    dimension_column=dimension_column,
                    metric_column="row_count",
                    ascending=False,
                ),
            }
        )
    if pending_count:
        ranking_sections.append(
            {
                "title": f"Highest backlog by {_format_label(pending_count)}",
                "metric": pending_count,
                "items": _top_rankings(
                    compare_df,
                    dimension_column=dimension_column,
                    metric_column=pending_count,
                    ascending=False,
                ),
            }
        )
    if pending_rate:
        valid_pending_rate = compare_df[pd.to_numeric(compare_df[pending_rate], errors="coerce").notna()].copy()
        if not valid_pending_rate.empty:
            ranking_sections.append(
                {
                    "title": f"Lowest {_format_label(pending_rate)}",
                    "metric": pending_rate,
                    "items": _top_rankings(
                        valid_pending_rate,
                        dimension_column=dimension_column,
                        metric_column=pending_rate,
                        ascending=True,
                        secondary_column=pending_count if pending_count else None,
                    ),
                }
            )
            ranking_sections.append(
                {
                    "title": f"Highest {_format_label(pending_rate)}",
                    "metric": pending_rate,
                    "items": _top_rankings(
                        valid_pending_rate,
                        dimension_column=dimension_column,
                        metric_column=pending_rate,
                        ascending=False,
                        secondary_column=pending_count if pending_count else None,
                    ),
                }
            )
    if primary_rate:
        valid_primary_rate = compare_df[pd.to_numeric(compare_df[primary_rate], errors="coerce").notna()].copy()
        if not valid_primary_rate.empty:
            ranking_sections.append(
                {
                    "title": f"Lowest {_format_label(primary_rate)}",
                    "metric": primary_rate,
                    "items": _top_rankings(
                        valid_primary_rate,
                        dimension_column=dimension_column,
                        metric_column=primary_rate,
                        ascending=True,
                        secondary_column=disposed_count if disposed_count else None,
                    ),
                }
            )

    takeaways: list[str] = []
    if primary_count and not compare_df.empty:
        measure_series = pd.to_numeric(compare_df[primary_count], errors="coerce")
        top_idx = measure_series.idxmax()
        if pd.notna(top_idx):
            top_row = compare_df.loc[top_idx]
            denominator = None
            if overall_row is not None and pd.notna(overall_row.get(primary_count)):
                denominator = pd.to_numeric(pd.Series([overall_row[primary_count]]), errors="coerce").iloc[0]
            elif pd.notna(measure_series.sum()) and measure_series.sum() > 0:
                denominator = float(measure_series.sum())

            if denominator and denominator > 0:
                share = float(top_row[primary_count]) / float(denominator) * 100
                takeaways.append(
                    f"{_format_dimension_value(top_row[dimension_column])} is the largest contributor in {_format_label(primary_count)}, accounting for about {share:.1f}% of the total."
                )
    elif "row_count" in compare_df.columns and not compare_df.empty:
        count_series = pd.to_numeric(compare_df["row_count"], errors="coerce")
        if count_series.notna().any() and float(count_series.sum()) > 0:
            top_idx = count_series.idxmax()
            top_row = compare_df.loc[top_idx]
            share = float(top_row["row_count"]) / float(count_series.sum()) * 100
            takeaways.append(
                f"{_format_dimension_value(top_row[dimension_column])} is the most common {_format_label(dimension_column)} value, representing about {share:.1f}% of the rows."
            )

    if pending_count and pending_rate and not detail_df.empty:
        pending_series = pd.to_numeric(compare_df[pending_count], errors="coerce")
        rate_series = pd.to_numeric(compare_df[pending_rate], errors="coerce")
        if pending_series.notna().any():
            pending_idx = pending_series.idxmax()
            pending_row = compare_df.loc[pending_idx]
            takeaways.append(
                f"The biggest backlog sits in {_format_dimension_value(pending_row[dimension_column])} with {_format_value(pending_row[pending_count])} pending cases."
            )
        if rate_series.notna().any():
            highest_rate_idx = rate_series.idxmax()
            highest_rate_row = compare_df.loc[highest_rate_idx]
            takeaways.append(
                f"The highest {_format_label(pending_rate)} appears in {_format_dimension_value(highest_rate_row[dimension_column])} at {_format_value(highest_rate_row[pending_rate])}%."
            )

    if primary_rate and not compare_df.empty:
        rate_series = pd.to_numeric(compare_df[primary_rate], errors="coerce")
        if rate_series.notna().any():
            low_idx = rate_series.idxmin()
            low_row = compare_df.loc[low_idx]
            takeaways.append(
                f"{_format_dimension_value(low_row[dimension_column])} has the weakest {_format_label(primary_rate)} at {_format_value(low_row[primary_rate])}%."
            )

    if not takeaways:
        if numeric_columns:
            takeaways.append("This dataset can be compared meaningfully by the main categorical field, but no standout story was detected automatically.")
        else:
            takeaways.append("This dataset is mostly categorical, so the most useful first step is comparing label counts and response patterns.")

    consistency_checks = _build_consistency_checks(
        detail_df=detail_df,
        overall_row=overall_row,
        metric_choices=metric_choices,
        numeric_columns=numeric_columns,
        schema=schema,
    )
    anomaly_flags = _build_anomaly_flags(
        detail_df=detail_df,
        dimension_column=dimension_column,
        metric_choices=metric_choices,
        numeric_columns=numeric_columns,
        schema=schema,
    )

    return {
        "intent": dataset_intent,
        "focus_dimension": dimension_column,
        "overview": overview,
        "headline_metrics": headline_metrics,
        "ranking_sections": ranking_sections,
        "takeaways": takeaways[:4],
        "metric_choices": metric_choices,
        "consistency_checks": consistency_checks,
        "anomaly_flags": anomaly_flags,
        "detail_row_count": int(len(detail_df)),
        "summary_row_count": int(len(total_df)),
    }


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
    dataset_story = _build_dataset_story(df, schema, numeric_columns)

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
    for takeaway in dataset_story.get("takeaways", [])[:2]:
        key_insights.append(takeaway)
    if group_comparison and group_comparison["largest_mean_gaps"]:
        standout = group_comparison["largest_mean_gaps"][0]
        key_insights.append(
            f"Largest group gap in {group_comparison['group_column']}: {standout['metric']} differs by "
            f"{standout['difference']} between {standout['highest_group']} and {standout['lowest_group']}."
        )
    if not key_insights:
        key_insights.append("No standout relationships were available for this dataset shape.")

    warnings: list[str] = []
    for check in dataset_story.get("consistency_checks", []):
        if check.get("status") == "warning":
            warnings.append(check["message"])
    warnings.extend(dataset_story.get("anomaly_flags", []))
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
        "dataset_story": dataset_story,
    }
