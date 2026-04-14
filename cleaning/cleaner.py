from __future__ import annotations

from typing import Any

import pandas as pd

from schema.detect import infer_column_type


def clean_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    cleaned = df.copy()
    report: dict[str, Any] = {
        "duplicates_removed": 0,
        "missing_values_filled": {},
        "coerced_columns": [],
        "text_standardized": [],
    }

    before = len(cleaned)
    cleaned = cleaned.drop_duplicates()
    report["duplicates_removed"] = int(before - len(cleaned))

    for column in cleaned.columns:
        inferred_type = infer_column_type(cleaned[column])

        if inferred_type == "numeric":
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")
            fill_value = cleaned[column].median()
            if pd.isna(fill_value):
                fill_value = 0
            missing = int(cleaned[column].isna().sum())
            if missing:
                cleaned[column] = cleaned[column].fillna(fill_value)
                report["missing_values_filled"][column] = {
                    "strategy": "median",
                    "count": missing,
                    "value": float(fill_value),
                }
            report["coerced_columns"].append(column)
            continue

        if inferred_type == "datetime":
            converted = pd.to_datetime(cleaned[column], errors="coerce", format="mixed")
            if converted.notna().any():
                missing = int(converted.isna().sum())
                if missing:
                    converted = converted.ffill().bfill()
                    report["missing_values_filled"][column] = {
                        "strategy": "forward/back fill",
                        "count": missing,
                        "value": "neighbor value",
                    }
                cleaned[column] = converted
                report["coerced_columns"].append(column)
            continue

        cleaned[column] = cleaned[column].astype(str).str.strip()
        mode = cleaned[column].mode(dropna=True)
        missing_mask = cleaned[column].isin(["", "nan", "None"])
        missing = int(missing_mask.sum())
        if missing:
            fill_value = mode.iloc[0] if not mode.empty else "unknown"
            cleaned.loc[missing_mask, column] = fill_value
            report["missing_values_filled"][column] = {
                "strategy": "mode",
                "count": missing,
                "value": str(fill_value),
            }
        cleaned[column] = cleaned[column].str.lower().str.strip()
        report["text_standardized"].append(column)

    return cleaned, report
