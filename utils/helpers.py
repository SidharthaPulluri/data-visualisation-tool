from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import pandas as pd


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def normalize_headers(headers: list[Any]) -> list[str]:
    seen: dict[str, int] = {}
    normalized: list[str] = []

    for index, header in enumerate(headers, start=1):
        value = str(header).strip() if header is not None else ""
        if not value or value.lower().startswith("unnamed"):
            value = f"column_{index}"

        value = re.sub(r"\s+", "_", value)
        value = re.sub(r"[^a-zA-Z0-9_]", "", value).strip("_") or f"column_{index}"

        count = seen.get(value, 0)
        seen[value] = count + 1
        normalized.append(value if count == 0 else f"{value}_{count + 1}")

    return normalized


def sanitize_filename_stem(value: str) -> str:
    stem = re.sub(r"[^a-zA-Z0-9_-]+", "_", value)
    return stem.strip("_").lower() or "export"


def dataframe_preview(df: pd.DataFrame, limit: int = 12) -> list[dict[str, Any]]:
    preview_df = df.head(limit).copy()
    for column in preview_df.columns:
        if pd.api.types.is_datetime64_any_dtype(preview_df[column]):
            preview_df[column] = preview_df[column].dt.strftime("%Y-%m-%d %H:%M:%S")
    preview_df = preview_df.where(pd.notnull(preview_df), None)
    return preview_df.to_dict(orient="records")


def chart_options_for_type(column_type: str) -> list[str]:
    mapping = {
        "numeric": ["histogram", "box", "scatter"],
        "categorical": ["bar", "pie"],
        "datetime": ["line", "area"],
        "text": ["bar", "pie"],
    }
    return mapping.get(column_type, [])


def make_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: make_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [make_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [make_json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        try:
            return make_json_safe(value.item())
        except Exception:
            return str(value)
    return value
