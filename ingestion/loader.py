from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
from werkzeug.datastructures import FileStorage

from utils.helpers import normalize_headers

SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".json"}


def _read_csv(file: FileStorage) -> pd.DataFrame:
    file.stream.seek(0)
    raw_bytes = file.read()
    file.stream.seek(0)

    for encoding in ("utf-8", "utf-8-sig", "latin1"):
        try:
            return pd.read_csv(BytesIO(raw_bytes), encoding=encoding)
        except UnicodeDecodeError:
            continue

    return pd.read_csv(BytesIO(raw_bytes))


def _read_excel(file: FileStorage) -> pd.DataFrame:
    file.stream.seek(0)
    return pd.read_excel(file.stream)


def _read_json(file: FileStorage) -> pd.DataFrame:
    file.stream.seek(0)
    try:
        return pd.read_json(file.stream)
    except ValueError:
        file.stream.seek(0)
        return pd.read_json(file.stream, lines=True)


def load_uploaded_dataset(file: FileStorage) -> tuple[pd.DataFrame, dict[str, Any]]:
    extension = Path(file.filename or "").suffix.lower()
    if extension not in SUPPORTED_EXTENSIONS:
        raise ValueError("Unsupported file type. Use CSV, XLSX, or JSON.")

    if extension == ".csv":
        df = _read_csv(file)
    elif extension == ".xlsx":
        df = _read_excel(file)
    else:
        df = _read_json(file)

    original_headers = list(df.columns)
    df.columns = normalize_headers(original_headers)
    df = df.dropna(axis=1, how="all")

    report = {
        "file_type": extension.replace(".", ""),
        "rows_loaded": int(df.shape[0]),
        "columns_loaded": int(df.shape[1]),
        "header_fixes": [
            f"{before} -> {after}"
            for before, after in zip(original_headers, df.columns)
            if str(before) != str(after)
        ],
    }
    return df, report
