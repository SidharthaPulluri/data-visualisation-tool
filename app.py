from __future__ import annotations

import base64
from io import BytesIO, StringIO
from typing import Any
from uuid import uuid4

import pandas as pd
from flask import Flask, jsonify, request, send_file, send_from_directory

from analysis.stats import build_analysis_report
from cleaning.cleaner import clean_dataframe
from ingestion.loader import load_uploaded_dataset
from schema.detect import detect_schema
from transformation.transform import apply_transformations
from utils.helpers import dataframe_preview, sanitize_filename_stem
from visualization.plots import create_chart

BASE_DIR = __import__("pathlib").Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR / "frontend"

app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="/static")
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024

DATASETS: dict[str, dict[str, Any]] = {}


def _serialize_dataset_state(
    *,
    filename: str,
    clean_df: pd.DataFrame,
    load_report: dict[str, Any],
    cleaning_report: dict[str, Any],
) -> dict[str, Any]:
    return {
        "filename": filename,
        "clean_data": clean_df.to_json(orient="split", date_format="iso"),
        "load_report": load_report,
        "cleaning_report": cleaning_report,
    }


def _resolve_dataset(payload: dict[str, Any]) -> tuple[str, pd.DataFrame, dict[str, Any], dict[str, Any]]:
    dataset_state = payload.get("dataset_state")
    if dataset_state:
        clean_df = pd.read_json(StringIO(dataset_state["clean_data"]), orient="split")
        return (
            dataset_state.get("filename", "dataset"),
            clean_df,
            dataset_state.get("load_report", {}),
            dataset_state.get("cleaning_report", {}),
        )

    dataset_id = payload.get("dataset_id")
    if dataset_id in DATASETS:
        dataset = DATASETS[dataset_id]
        return dataset["filename"], dataset["clean_df"], dataset["load_report"], dataset["cleaning_report"]

    raise ValueError("Dataset session not found. Upload a file first.")


def _build_transform_payload(
    *,
    filename: str,
    clean_df: pd.DataFrame,
    load_report: dict[str, Any],
    cleaning_report: dict[str, Any],
    dataset_id: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    transformed_df, steps = apply_transformations(clean_df, config or {})
    schema = detect_schema(transformed_df)
    analysis = build_analysis_report(transformed_df, schema)
    return {
        "dataset_id": dataset_id,
        "filename": filename,
        "shape": {"rows": int(transformed_df.shape[0]), "columns": int(transformed_df.shape[1])},
        "schema": schema,
        "preview": dataframe_preview(transformed_df),
        "analysis": analysis,
        "steps": steps,
        "dataset_state": _serialize_dataset_state(
            filename=filename,
            clean_df=clean_df,
            load_report=load_report,
            cleaning_report=cleaning_report,
        ),
    }


@app.get("/")
def index() -> Any:
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.get("/health")
def health() -> Any:
    return jsonify({"status": "ok"})


@app.post("/api/upload")
def upload_dataset() -> Any:
    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"error": "Please choose a CSV, Excel, or JSON file."}), 400

    try:
        raw_df, load_report = load_uploaded_dataset(file)
        raw_schema = detect_schema(raw_df)
        clean_df, cleaning_report = clean_dataframe(raw_df)
        clean_schema = detect_schema(clean_df)
        analysis = build_analysis_report(clean_df, clean_schema)

        dataset_id = uuid4().hex
        DATASETS[dataset_id] = {
            "filename": file.filename,
            "raw_df": raw_df,
            "clean_df": clean_df,
            "load_report": load_report,
            "cleaning_report": cleaning_report,
        }

        return jsonify(
            {
                "dataset_id": dataset_id,
                "filename": file.filename,
                "shape": {"rows": int(clean_df.shape[0]), "columns": int(clean_df.shape[1])},
                "load_report": load_report,
                "cleaning_report": cleaning_report,
                "raw_schema": raw_schema,
                "schema": clean_schema,
                "preview": dataframe_preview(clean_df),
                "analysis": analysis,
                "dataset_state": _serialize_dataset_state(
                    filename=file.filename,
                    clean_df=clean_df,
                    load_report=load_report,
                    cleaning_report=cleaning_report,
                ),
            }
        )
    except ValueError as error:
        return jsonify({"error": str(error)}), 400


@app.post("/api/transform")
def transform_dataset() -> Any:
    payload = request.get_json(silent=True) or {}
    try:
        filename, clean_df, load_report, cleaning_report = _resolve_dataset(payload)
        return jsonify(
            _build_transform_payload(
                filename=filename,
                clean_df=clean_df,
                load_report=load_report,
                cleaning_report=cleaning_report,
                dataset_id=payload.get("dataset_id"),
                config=payload.get("config"),
            )
        )
    except ValueError as error:
        return jsonify({"error": str(error)}), 404


@app.post("/api/visualize")
def visualize_dataset() -> Any:
    payload = request.get_json(silent=True) or {}
    config = payload.get("config") or {}
    chart_type = payload.get("chart_type")
    x_column = payload.get("x_column")
    y_column = payload.get("y_column")
    title = payload.get("title")
    output_format = str(payload.get("format", "png")).lower()

    if not chart_type or not x_column:
        return jsonify({"error": "Chart type and X column are required."}), 400

    try:
        _, clean_df, _, _ = _resolve_dataset(payload)
        transformed_df, _ = apply_transformations(clean_df, config)
        transformed_schema = detect_schema(transformed_df)
        chart_bytes = create_chart(
            df=transformed_df,
            schema=transformed_schema,
            chart_type=chart_type,
            x_column=x_column,
            y_column=y_column,
            title=title,
            output_format=output_format,
        )
        mime_type = "application/pdf" if output_format == "pdf" else "image/png"
        encoded_chart = base64.b64encode(chart_bytes).decode("ascii")
        download_name = sanitize_filename_stem(f"{payload.get('title') or chart_type}_{uuid4().hex[:8]}") + f".{output_format}"

        return jsonify(
            {
                "chart_data_url": f"data:{mime_type};base64,{encoded_chart}",
                "chart_file": download_name,
                "mime_type": mime_type,
                "shape": {"rows": int(transformed_df.shape[0]), "columns": int(transformed_df.shape[1])},
                "schema": transformed_schema,
            }
        )
    except ValueError as error:
        return jsonify({"error": str(error)}), 400


@app.post("/api/export/dataset")
def export_dataset() -> Any:
    payload = request.get_json(silent=True) or {}
    config = payload.get("config") or {}
    export_format = str(payload.get("format", "csv")).lower()

    try:
        filename, clean_df, _, _ = _resolve_dataset(payload)
    except ValueError as error:
        return jsonify({"error": str(error)}), 404

    transformed_df, _ = apply_transformations(clean_df, config)
    stem = sanitize_filename_stem(f"{filename}_cleaned")

    if export_format == "xlsx":
        output = BytesIO()
        transformed_df.to_excel(output, index=False)
        output.seek(0)
        download_name = f"{stem}.xlsx"
        mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        output = BytesIO(transformed_df.to_csv(index=False).encode("utf-8"))
        download_name = f"{stem}.csv"
        mimetype = "text/csv"

    return send_file(output, as_attachment=True, download_name=download_name, mimetype=mimetype)


@app.post("/api/export/report")
def export_report() -> Any:
    payload = request.get_json(silent=True) or {}
    config = payload.get("config") or {}

    try:
        filename, clean_df, load_report, cleaning_report = _resolve_dataset(payload)
    except ValueError as error:
        return jsonify({"error": str(error)}), 404

    transformed_df, steps = apply_transformations(clean_df, config)
    schema = detect_schema(transformed_df)
    analysis = build_analysis_report(transformed_df, schema)

    report_lines = [
        "Rule-Based Data Analysis Report",
        "=" * 32,
        f"Source file: {filename}",
        f"Rows: {transformed_df.shape[0]}",
        f"Columns: {transformed_df.shape[1]}",
        "",
        "Load report:",
        f"- File type: {load_report['file_type']}",
        f"- Header fixes: {load_report['header_fixes']}",
        "",
        "Cleaning report:",
        f"- Removed duplicates: {cleaning_report['duplicates_removed']}",
        f"- Missing values filled: {cleaning_report['missing_values_filled']}",
        f"- Columns coerced: {', '.join(cleaning_report['coerced_columns']) or 'none'}",
        "",
        "Transformation steps:",
    ]

    report_lines.extend(f"- {step}" for step in steps or ["No transformation applied"])
    report_lines.extend(["", "Schema summary:"])
    report_lines.extend(
        f"- {column}: {meta['type']} ({meta['dtype']})" for column, meta in schema.items()
    )
    report_lines.extend(["", "Analysis summary:", analysis["summary_lines"]])

    report_bytes = BytesIO("\n".join(report_lines).encode("utf-8"))
    report_name = f"{sanitize_filename_stem(filename)}_report.txt"
    return send_file(report_bytes, as_attachment=True, download_name=report_name, mimetype="text/plain")


if __name__ == "__main__":
    app.run(debug=True)
