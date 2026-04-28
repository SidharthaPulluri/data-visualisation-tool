from __future__ import annotations

import csv
import re
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any

import pandas as pd
from werkzeug.datastructures import FileStorage

from utils.helpers import normalize_headers

SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".data", ".xlsx", ".xls", ".json"}


def _decode_text(raw_bytes: bytes) -> tuple[str, str]:
    for encoding in ("utf-8", "utf-8-sig", "latin1"):
        try:
            return raw_bytes.decode(encoding), encoding
        except UnicodeDecodeError:
            continue

    return raw_bytes.decode("utf-8", errors="replace"), "utf-8"


def _looks_like_unparsed_delimited_frame(df: pd.DataFrame) -> bool:
    if df.shape[1] != 1:
        return False

    only_column = str(df.columns[0])
    if any(delimiter in only_column for delimiter in (",", ";", "\t", "|")):
        return True

    series = df.iloc[:, 0].dropna().astype(str).head(12)
    return any(any(delimiter in value for delimiter in (",", ";", "\t", "|")) for value in series)


def _generated_header_names(df: pd.DataFrame) -> list[str]:
    if df.shape[1] == 0:
        return []

    names: list[str] = []
    first_series = df.iloc[:, 0].dropna().astype(str).str.strip().str.lower()
    first_values = set(first_series.head(50))
    if {"republican", "democrat"}.intersection(first_values):
        names.append("party")
    elif {"m", "b", "yes", "no", "y", "n"}.intersection(first_values) and first_series.nunique() <= 8:
        names.append("class_label")
    else:
        names.append("label")

    remaining_columns = df.iloc[:, 1:]
    if remaining_columns.empty:
        return names

    binary_tokens = {"y", "n", "yes", "no", "?", "0", "1", "t", "f", "true", "false"}
    binary_like_count = 0
    for column in remaining_columns.columns:
        values = set(
            remaining_columns[column]
            .dropna()
            .astype(str)
            .str.strip()
            .str.lower()
            .head(50)
        )
        if values and values.issubset(binary_tokens):
            binary_like_count += 1

    if binary_like_count >= max(3, int(remaining_columns.shape[1] * 0.6)):
        prefix = "vote"
    else:
        prefix = "feature"

    names.extend(f"{prefix}_{index}" for index in range(1, remaining_columns.shape[1] + 1))
    return names


def _headers_look_like_values(df: pd.DataFrame) -> bool:
    if df.shape[1] < 3:
        return False

    header_tokens = [str(column).strip().lower() for column in df.columns]
    if not any(header_tokens):
        return True

    short_ratio = sum(len(token) <= 3 for token in header_tokens) / len(header_tokens)
    duplicate_suffix_ratio = sum(bool(re.search(r"\.\d+$", token)) for token in header_tokens) / len(header_tokens)
    value_like_tokens = {
        "",
        "?",
        "-",
        "na",
        "nan",
        "null",
        "y",
        "n",
        "yes",
        "no",
        "t",
        "f",
        "true",
        "false",
        "present",
        "absent",
        "democrat",
        "republican",
        "m",
        "b",
    }
    value_like_ratio = sum(token in value_like_tokens for token in header_tokens) / len(header_tokens)
    numeric_like_ratio = sum(token.replace(".", "", 1).isdigit() for token in header_tokens) / len(header_tokens)
    base_tokens = [re.sub(r"\.\d+$", "", token) for token in header_tokens]
    repeated_base_ratio = 1 - (len(set(base_tokens)) / len(base_tokens))

    score = 0
    score += 1 if short_ratio >= 0.6 else 0
    score += 1 if duplicate_suffix_ratio >= 0.2 else 0
    score += 1 if value_like_ratio >= 0.3 else 0
    score += 1 if numeric_like_ratio >= 0.4 else 0
    score += 1 if repeated_base_ratio >= 0.25 else 0
    return score >= 2


def _parse_delimited_text(
    text: str,
    *,
    delimiter: str,
    headerless: bool = False,
) -> pd.DataFrame:
    read_kwargs: dict[str, Any] = {
        "sep": delimiter,
        "engine": "python",
        "skip_blank_lines": True,
        "on_bad_lines": "skip",
    }
    if headerless:
        read_kwargs["header"] = None

    df = pd.read_csv(StringIO(text), **read_kwargs)
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")

    if headerless:
        df.columns = _generated_header_names(df)

    return df


def _read_delimited(
    file: FileStorage,
    separator: str = ",",
    *,
    allow_headerless: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    file.stream.seek(0)
    raw_bytes = file.read()
    file.stream.seek(0)

    text, encoding = _decode_text(raw_bytes)
    if not text.strip():
        raise ValueError("The uploaded file is empty.")

    candidate_separators: list[str] = [separator]
    try:
        sniffed = csv.Sniffer().sniff(text[:4096], delimiters=",;\t|").delimiter
        candidate_separators.append(sniffed)
    except csv.Error:
        pass

    for delimiter in (",", ";", "\t", "|"):
        candidate_separators.append(delimiter)

    seen: set[str] = set()
    for candidate in candidate_separators:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            df = _parse_delimited_text(text, delimiter=candidate)
        except (pd.errors.EmptyDataError, pd.errors.ParserError, UnicodeDecodeError, ValueError):
            continue

        if df.empty:
            continue
        if _looks_like_unparsed_delimited_frame(df):
            continue

        header_mode = "parsed"
        if allow_headerless and _headers_look_like_values(df):
            try:
                headerless_df = _parse_delimited_text(text, delimiter=candidate, headerless=True)
            except (pd.errors.EmptyDataError, pd.errors.ParserError, UnicodeDecodeError, ValueError):
                headerless_df = pd.DataFrame()

            if not headerless_df.empty and not _looks_like_unparsed_delimited_frame(headerless_df):
                df = headerless_df
                header_mode = "generated"

        return df, {"encoding_used": encoding, "delimiter_used": candidate, "header_mode": header_mode}

    raise ValueError("No valid data parsed from the delimited text file. Check that it uses a consistent delimiter and contains usable rows.")


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
        raise ValueError("Unsupported file type. Use CSV, TSV, DATA, XLSX, XLS, or JSON.")

    if extension == ".csv":
        df, parse_report = _read_delimited(file, separator=",", allow_headerless=True)
    elif extension == ".tsv":
        df, parse_report = _read_delimited(file, separator="\t", allow_headerless=True)
    elif extension == ".data":
        df, parse_report = _read_delimited(file, separator=",", allow_headerless=True)
    elif extension in {".xlsx", ".xls"}:
        df = _read_excel(file, extension)
        parse_report = {}
    else:
        df = _read_json(file)
        parse_report = {}

    original_headers = list(df.columns)
    df.columns = normalize_headers(original_headers)
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")

    if df.empty or df.shape[1] == 0:
        raise ValueError("No valid data parsed from the uploaded file.")

    report = {
        "file_type": extension.replace(".", ""),
        "rows_loaded": int(df.shape[0]),
        "columns_loaded": int(df.shape[1]),
        **parse_report,
        "header_fixes": [
            f"{before} -> {after}"
            for before, after in zip(original_headers, df.columns)
            if str(before) != str(after)
        ],
    }
    return df, report
