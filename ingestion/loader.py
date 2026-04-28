from __future__ import annotations

import csv
import json
import re
import zipfile
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any

import pandas as pd
from werkzeug.datastructures import FileStorage

from utils.helpers import normalize_headers

SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".data", ".xlsx", ".xls", ".json", ".zip"}


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
    parser_notes: list[str] = []
    for candidate in candidate_separators:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            df = _parse_delimited_text(text, delimiter=candidate)
        except (pd.errors.EmptyDataError, pd.errors.ParserError, UnicodeDecodeError, ValueError):
            continue

        if df.empty:
            parser_notes.append(f"Tried delimiter {repr(candidate)} but no usable rows were found.")
            continue
        if _looks_like_unparsed_delimited_frame(df):
            parser_notes.append(f"Tried delimiter {repr(candidate)} but the file still looked like one unparsed text column.")
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
                parser_notes.append("The file looked headerless, so generated column names were applied.")

        return df, {
            "encoding_used": encoding,
            "delimiter_used": candidate,
            "header_mode": header_mode,
            "parser_notes": parser_notes,
        }

    raise ValueError("No valid data parsed from the delimited text file. Check that it uses a consistent delimiter and contains usable rows.")


def _sheet_quality_score(df: pd.DataFrame) -> tuple[int, int, int]:
    cleaned = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    if cleaned.empty or cleaned.shape[1] == 0:
        return (0, 0, 0)

    numeric_columns = sum(pd.api.types.is_numeric_dtype(cleaned[column]) for column in cleaned.columns)
    return (int(cleaned.shape[0] * cleaned.shape[1]), int(cleaned.shape[1]), int(numeric_columns))


def _sheet_name_bias(sheet_name: str) -> int:
    tokens = {token for token in re.split(r"[_\W]+", sheet_name.lower()) if token}
    positive_tokens = {"data", "dataset", "export", "records", "table", "results", "values", "raw"}
    negative_tokens = {"note", "notes", "readme", "instruction", "instructions", "info", "information", "metadata", "cover", "contents", "legend"}

    return len(tokens.intersection(positive_tokens)) * 6 - len(tokens.intersection(negative_tokens)) * 8


def _sheet_long_text_penalty(df: pd.DataFrame) -> int:
    cleaned = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    if cleaned.empty:
        return 0

    sample = cleaned.astype(str).stack().head(120)
    if sample.empty:
        return 0

    long_text_ratio = sample.str.len().gt(36).mean()
    return int(long_text_ratio * 100)


def _rank_excel_sheet(sheet_name: str, df: pd.DataFrame) -> tuple[int, int, int, int, int]:
    area, width, numeric_columns = _sheet_quality_score(df)
    name_bias = _sheet_name_bias(sheet_name)
    structure_bonus = 8 if area and width >= 3 and len(df) >= 3 else 0
    long_text_penalty = _sheet_long_text_penalty(df)
    return (name_bias, structure_bonus, area, numeric_columns, -long_text_penalty)


def _read_excel(file: FileStorage, extension: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    file.stream.seek(0)
    try:
        read_kwargs: dict[str, Any] = {"sheet_name": None}
        if extension == ".xls":
            read_kwargs["engine"] = "xlrd"
        workbook = pd.read_excel(file.stream, **read_kwargs)
    except ImportError as error:
        if extension == ".xls":
            raise ValueError("Older Excel .xls files need the 'xlrd' package installed before they can be uploaded.") from error
        raise

    if isinstance(workbook, pd.DataFrame):
        return workbook, {"excel_sheet_name": "Sheet1", "parser_notes": []}

    sheets = {
        str(sheet_name): sheet_df.dropna(axis=0, how="all").dropna(axis=1, how="all")
        for sheet_name, sheet_df in workbook.items()
    }
    usable_sheets = {name: df for name, df in sheets.items() if not df.empty and df.shape[1] > 0}
    if not usable_sheets:
        raise ValueError("The uploaded Excel workbook did not contain any usable sheets.")

    ranked_sheets = sorted(
        usable_sheets.items(),
        key=lambda item: (*_rank_excel_sheet(item[0], item[1]), -len(item[0])),
        reverse=True,
    )
    selected_sheet_name, selected_df = ranked_sheets[0]
    parser_notes: list[str] = []
    if len(usable_sheets) > 1:
        parser_notes.append(
            f"Loaded worksheet '{selected_sheet_name}' from a workbook with {len(usable_sheets)} usable sheets."
        )
    return selected_df, {
        "excel_sheet_name": selected_sheet_name,
        "excel_sheet_names": list(sheets.keys()),
        "excel_sheet_rankings": [name for name, _ in ranked_sheets[:5]],
        "parser_notes": parser_notes,
    }


def _flatten_json_record(record: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in record.items():
        safe_key = str(key).strip() or "field"
        compound_key = f"{prefix}__{safe_key}" if prefix else safe_key
        if isinstance(value, dict):
            flattened.update(_flatten_json_record(value, compound_key))
        elif isinstance(value, list):
            if all(not isinstance(item, (dict, list)) for item in value):
                flattened[compound_key] = " | ".join("" if item is None else str(item) for item in value)
            else:
                flattened[compound_key] = json.dumps(value, ensure_ascii=False)
        else:
            flattened[compound_key] = value
    return flattened


def _coerce_json_frame(payload: Any, *, json_path: tuple[str, ...] = ()) -> tuple[pd.DataFrame, dict[str, Any]] | None:
    parser_notes: list[str] = []

    if isinstance(payload, list):
        dict_count = sum(isinstance(item, dict) for item in payload)
        if payload and dict_count == len(payload):
            flattened_records = [_flatten_json_record(item) for item in payload]
            report = {"json_mode": "record_array", "parser_notes": parser_notes}
            if json_path:
                report["json_path"] = ".".join(json_path)
            return pd.DataFrame(flattened_records), report

        if payload and dict_count >= max(1, int(len(payload) * 0.6)):
            flattened_records: list[dict[str, Any]] = []
            for item in payload:
                if isinstance(item, dict):
                    flattened_records.append(_flatten_json_record(item))
                else:
                    flattened_records.append({"_value": item})
            parser_notes.append("Mixed JSON array was treated as record data; non-object items were preserved in '_value'.")
            report = {"json_mode": "mixed_record_array", "parser_notes": parser_notes}
            if json_path:
                report["json_path"] = ".".join(json_path)
            return pd.DataFrame(flattened_records), report

        if payload and all(isinstance(item, list) for item in payload):
            header_row = payload[0]
            data_rows = payload[1:]
            if (
                header_row
                and all(not isinstance(item, (list, dict)) for item in header_row)
                and data_rows
            ):
                width = len(header_row)
                if width and all(len(row) == width for row in data_rows[:100] if isinstance(row, list)):
                    columns = [
                        str(item).strip() if str(item).strip() else f"column_{index + 1}"
                        for index, item in enumerate(header_row)
                    ]
                    parser_notes.append("JSON array was interpreted as a header row followed by records.")
                    report = {
                        "json_mode": "array_with_header_row",
                        "parser_notes": parser_notes,
                    }
                    if json_path:
                        report["json_path"] = ".".join(json_path)
                    return pd.DataFrame(data_rows, columns=columns), report

            report = {"json_mode": "array_rows", "parser_notes": parser_notes}
            if json_path:
                report["json_path"] = ".".join(json_path)
            return pd.DataFrame(payload), report

    if isinstance(payload, dict):
        for key in ("data", "results", "items", "records"):
            value = payload.get(key)
            if isinstance(value, list):
                frame, report = _coerce_json_frame(value, json_path=(*json_path, key)) or (None, None)
                if frame is not None and report is not None:
                    report["json_container_key"] = key
                    report.setdefault("parser_notes", []).append(
                        f"Loaded records from JSON key path '{'.'.join((*json_path, key))}'."
                    )
                    return frame, report

        object_candidate: tuple[pd.DataFrame, dict[str, Any]] | None = None
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                nested = _coerce_json_frame(value, json_path=(*json_path, str(key)))
                if nested is not None:
                    frame, report = nested
                    if report.get("json_mode") != "object":
                        report.setdefault("parser_notes", []).append(
                            f"Nested JSON object '{'.'.join((*json_path, str(key)))}' was used as the dataset source."
                        )
                        return frame, report
                    if object_candidate is None:
                        object_candidate = (
                            frame,
                            {
                                **report,
                                "parser_notes": list(report.get("parser_notes", []))
                                + [f"Nested JSON object '{'.'.join((*json_path, str(key)))}' was used as the dataset source."],
                            },
                        )

        if object_candidate is not None:
            return object_candidate

        report = {"json_mode": "object", "parser_notes": parser_notes}
        if json_path:
            report["json_path"] = ".".join(json_path)
        return pd.json_normalize(payload), report

    return None


def _read_json(file: FileStorage) -> tuple[pd.DataFrame, dict[str, Any]]:
    file.stream.seek(0)
    raw_bytes = file.read()
    file.stream.seek(0)
    text, encoding = _decode_text(raw_bytes)
    if not text.strip():
        raise ValueError("The uploaded file is empty.")

    try:
        payload = json.loads(text)
        coerced = _coerce_json_frame(payload)
        if coerced is not None:
            df, report = coerced
            report["encoding_used"] = encoding
            return df, report
    except json.JSONDecodeError:
        pass

    file.stream.seek(0)
    try:
        return pd.read_json(file.stream), {"encoding_used": encoding, "json_mode": "pandas", "parser_notes": []}
    except ValueError:
        file.stream.seek(0)
        return pd.read_json(file.stream, lines=True), {
            "encoding_used": encoding,
            "json_mode": "json_lines",
            "parser_notes": ["JSON Lines mode was used after standard JSON parsing failed."],
        }


def _read_zip_archive(file: FileStorage) -> tuple[pd.DataFrame, dict[str, Any]]:
    file.stream.seek(0)
    raw_bytes = file.read()
    file.stream.seek(0)

    try:
        archive = zipfile.ZipFile(BytesIO(raw_bytes))
    except zipfile.BadZipFile as error:
        raise ValueError("The uploaded ZIP file could not be read as a valid archive.") from error

    with archive:
        members = [info for info in archive.infolist() if not info.is_dir()]
        supported: list[tuple[int, int, zipfile.ZipInfo, str]] = []
        priority = {".csv": 0, ".tsv": 1, ".data": 2, ".xlsx": 3, ".xls": 4, ".json": 5}
        for info in members:
            extension = Path(info.filename).suffix.lower()
            if extension in (SUPPORTED_EXTENSIONS - {".zip"}):
                supported.append((priority.get(extension, 99), -info.file_size, info, extension))

        if not supported:
            raise ValueError("The ZIP archive did not contain a supported dataset file.")

        supported.sort()
        _, _, selected_info, selected_extension = supported[0]
        selected_name = Path(selected_info.filename).name
        nested_file = FileStorage(
            stream=BytesIO(archive.read(selected_info)),
            filename=selected_name,
            content_type="application/octet-stream",
        )
        df, report = load_uploaded_dataset(nested_file)
        archive_members = [info.filename for info in members]
        parser_notes = list(report.get("parser_notes", []))
        parser_notes.insert(0, f"Loaded dataset file '{selected_name}' from ZIP archive.")
        if len(supported) > 1:
            parser_notes.insert(1, "The archive contained multiple supported files, so the best candidate was selected automatically.")

        report.update(
            {
                "file_type": "zip",
                "archive_member": selected_info.filename,
                "archive_members": archive_members,
                "archive_member_type": selected_extension.replace(".", ""),
                "parser_notes": parser_notes,
            }
        )
        return df, report


def load_uploaded_dataset(file: FileStorage) -> tuple[pd.DataFrame, dict[str, Any]]:
    extension = Path(file.filename or "").suffix.lower()
    if extension not in SUPPORTED_EXTENSIONS:
        raise ValueError("Unsupported file type. Use CSV, TSV, DATA, XLSX, XLS, JSON, or ZIP.")

    if extension == ".zip":
        return _read_zip_archive(file)

    if extension == ".csv":
        df, parse_report = _read_delimited(file, separator=",", allow_headerless=True)
    elif extension == ".tsv":
        df, parse_report = _read_delimited(file, separator="\t", allow_headerless=True)
    elif extension == ".data":
        df, parse_report = _read_delimited(file, separator=",", allow_headerless=True)
    elif extension in {".xlsx", ".xls"}:
        df, parse_report = _read_excel(file, extension)
    else:
        df, parse_report = _read_json(file)

    original_row_count = int(df.shape[0])
    original_column_count = int(df.shape[1])
    original_headers = list(df.columns)
    duplicate_headers = sorted({str(header) for header in original_headers if original_headers.count(header) > 1})
    df.columns = normalize_headers(original_headers)
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")

    if df.empty or df.shape[1] == 0:
        raise ValueError("No valid data parsed from the uploaded file.")

    normalized_headers = list(df.columns)
    normalized_duplicates = sorted({str(header) for header in normalized_headers if normalized_headers.count(header) > 1})
    empty_rows_dropped = max(original_row_count - int(df.shape[0]), 0)
    empty_columns_dropped = max(original_column_count - int(df.shape[1]), 0)
    parser_notes = list(parse_report.get("parser_notes", []))
    if parse_report.get("delimiter_used"):
        parser_notes.append(f"Delimiter used: {parse_report['delimiter_used']!r}.")
    if parse_report.get("encoding_used"):
        parser_notes.append(f"Encoding used: {parse_report['encoding_used']}.")
    if parse_report.get("header_mode") == "generated":
        parser_notes.append("Generated column names were used because the first row looked like data, not headers.")
    if empty_rows_dropped:
        parser_notes.append(f"Dropped {empty_rows_dropped} completely empty row{'s' if empty_rows_dropped != 1 else ''}.")
    if empty_columns_dropped:
        parser_notes.append(f"Dropped {empty_columns_dropped} completely empty column{'s' if empty_columns_dropped != 1 else ''}.")
    if duplicate_headers:
        parser_notes.append("Duplicate source headers were detected and normalized into unique column names.")
    if normalized_duplicates:
        parser_notes.append("Some normalized column names still repeat and may need review before charting.")

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
        "duplicate_headers_detected": duplicate_headers,
        "normalized_duplicate_headers": normalized_duplicates,
        "empty_rows_dropped": empty_rows_dropped,
        "empty_columns_dropped": empty_columns_dropped,
        "parser_notes": parser_notes,
    }
    return df, report
