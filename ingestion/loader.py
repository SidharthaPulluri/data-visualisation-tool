from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
from werkzeug.datastructures import FileStorage

from utils.helpers import normalize_headers

SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".xlsx", ".xls", ".json"}


def _read_delimited(file: FileStorage, separator: str = ",") -> pd.DataFrame:
    file.stream.seek(0)
    raw_bytes = file.read()
    file.stream.seek(0)

    for encoding in ("utf-8", "utf-8-sig", "latin1"):
        try:
            return pd.read_csv(BytesIO(raw_bytes), encoding=encoding, sep=separator)
        except UnicodeDecodeError:
            continue

    return pd.read_csv(BytesIO(raw_bytes), sep=separator)


def _read_excel(file: FileStorage, extension: str) -> pd.DataFrame:
    file.stream.seek(0)
    try:
        if extension == ".xls":
            return pd.read_excel(file.stream, engine="xlrd")
        return pd.read_excel(file.stream)
    except ImportError as error:
        if extension == ".xls":
            raise ValueError("Older Excel .xls files need the 'xlrd' package installed before they can be uploaded.") from error
        raise


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
        raise ValueError("Unsupported file type. Use CSV, TSV, XLSX, XLS, or JSON.")

    if extension == ".csv":
        df = _read_delimited(file, separator=",")
    elif extension == ".tsv":
        df = _read_delimited(file, separator="\t")
    elif extension in {".xlsx", ".xls"}:
        df = _read_excel(file, extension)
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
