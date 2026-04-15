from __future__ import annotations

import base64
from io import BytesIO, StringIO
from typing import Any
from uuid import uuid4

import matplotlib.pyplot as plt
import pandas as pd
from flask import Flask, jsonify, request, send_file, send_from_directory
from matplotlib.backends.backend_pdf import PdfPages

from analysis.stats import build_analysis_report
from cleaning.cleaner import clean_dataframe
from ingestion.loader import load_uploaded_dataset
from schema.detect import detect_schema
from transformation.transform import apply_transformations
from utils.helpers import dataframe_preview, make_json_safe, sanitize_filename_stem
from visualization.plots import create_chart

BASE_DIR = __import__("pathlib").Path(__file__).resolve().parent
FRONTEND_DIR = BASE_DIR / "frontend"

app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="/static")
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024

DATASETS: dict[str, dict[str, Any]] = {}


def _json_response(payload: dict[str, Any], status_code: int = 200) -> Any:
    return jsonify(make_json_safe(payload)), status_code


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


def _format_report_value(value: Any, decimals: int = 4) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.{decimals}f}".rstrip("0").rstrip(".")
    return str(value)


def _schema_examples(schema: dict[str, dict[str, Any]], column_type: str, limit: int = 5) -> list[str]:
    return [column for column, meta in schema.items() if meta["type"] == column_type][:limit]


def _normalise_chart_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    charts = payload.get("charts")
    if not isinstance(charts, list):
        return []
    return [chart for chart in charts if isinstance(chart, dict) and chart.get("chart_type") and chart.get("x_column")]


@app.get("/")
def index() -> Any:
    return send_from_directory(FRONTEND_DIR, "upload.html")


@app.get("/prepare")
def prepare_page() -> Any:
    return send_from_directory(FRONTEND_DIR, "prepare.html")


@app.get("/visualize")
def visualize_page() -> Any:
    return send_from_directory(FRONTEND_DIR, "visualize.html")


@app.get("/health")
def health() -> Any:
    return _json_response({"status": "ok"})


@app.post("/api/upload")
def upload_dataset() -> Any:
    file = request.files.get("file")
    if not file or not file.filename:
        return _json_response({"error": "Please choose a CSV, Excel, or JSON file."}, 400)

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

        return _json_response(
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
        return _json_response({"error": str(error)}, 400)


@app.post("/api/transform")
def transform_dataset() -> Any:
    payload = request.get_json(silent=True) or {}
    try:
        filename, clean_df, load_report, cleaning_report = _resolve_dataset(payload)
        return _json_response(
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
        return _json_response({"error": str(error)}, 404)


@app.post("/api/visualize")
def visualize_dataset() -> Any:
    payload = request.get_json(silent=True) or {}
    config = payload.get("config") or {}
    chart_options = payload.get("chart_options") or {}
    chart_type = payload.get("chart_type")
    x_column = payload.get("x_column")
    y_column = payload.get("y_column")
    title = payload.get("title")
    output_format = str(payload.get("format", "png")).lower()

    if not chart_type or not x_column:
        return _json_response({"error": "Chart type and X column are required."}, 400)

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
            chart_options=chart_options,
        )
        mime_type = "application/pdf" if output_format == "pdf" else "image/png"
        encoded_chart = base64.b64encode(chart_bytes).decode("ascii")
        download_name = sanitize_filename_stem(f"{payload.get('title') or chart_type}_{uuid4().hex[:8]}") + f".{output_format}"

        return _json_response(
            {
                "chart_data_url": f"data:{mime_type};base64,{encoded_chart}",
                "chart_file": download_name,
                "mime_type": mime_type,
                "shape": {"rows": int(transformed_df.shape[0]), "columns": int(transformed_df.shape[1])},
                "schema": transformed_schema,
                "chart_options": chart_options,
            }
        )
    except ValueError as error:
        return _json_response({"error": str(error)}, 400)


@app.post("/api/export/dataset")
def export_dataset() -> Any:
    payload = request.get_json(silent=True) or {}
    config = payload.get("config") or {}
    export_format = str(payload.get("format", "csv")).lower()

    try:
        filename, clean_df, _, _ = _resolve_dataset(payload)
    except ValueError as error:
        return _json_response({"error": str(error)}, 404)

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


@app.post("/api/export/chart")
def export_chart() -> Any:
    payload = request.get_json(silent=True) or {}
    config = payload.get("config") or {}
    chart_options = payload.get("chart_options") or {}
    chart_type = payload.get("chart_type")
    x_column = payload.get("x_column")
    y_column = payload.get("y_column")
    title = payload.get("title")
    output_format = str(payload.get("format", "png")).lower()

    if not chart_type or not x_column:
        return _json_response({"error": "Chart type and X column are required."}, 400)

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
            chart_options=chart_options,
        )
    except ValueError as error:
        return _json_response({"error": str(error)}, 400)

    download_name = sanitize_filename_stem(f"{title or chart_type}_{uuid4().hex[:8]}") + f".{output_format}"
    mimetype = "application/pdf" if output_format == "pdf" else "image/png"
    return send_file(
        BytesIO(chart_bytes),
        as_attachment=True,
        download_name=download_name,
        mimetype=mimetype,
    )


@app.post("/api/export/dashboard")
def export_dashboard() -> Any:
    payload = request.get_json(silent=True) or {}
    config = payload.get("config") or {}
    charts = _normalise_chart_items(payload)

    if not charts:
        return _json_response({"error": "Save at least one chart before exporting a dashboard."}, 400)

    try:
        filename, clean_df, _, _ = _resolve_dataset(payload)
        transformed_df, _ = apply_transformations(clean_df, config)
        transformed_schema = detect_schema(transformed_df)
    except ValueError as error:
        return _json_response({"error": str(error)}, 404)

    output = BytesIO()
    with PdfPages(output) as pdf:
        cover = plt.figure(figsize=(11, 8.5))
        cover.text(0.08, 0.88, "Dashboard Export", fontsize=20, fontweight="bold")
        cover.text(0.08, 0.82, f"Source file: {filename}", fontsize=11)
        cover.text(0.08, 0.78, f"Saved charts included: {len(charts)}", fontsize=11)
        cover.text(
            0.08,
            0.71,
            "\n".join(
                f"{index + 1}. {chart.get('label') or chart.get('title') or chart.get('chart_type')}"
                for index, chart in enumerate(charts[:12])
            ),
            fontsize=11,
            va="top",
        )
        cover.gca().axis("off")
        pdf.savefig(cover, bbox_inches="tight")
        plt.close(cover)

        for chart in charts:
            title = chart.get("title") or chart.get("label") or chart["chart_type"].title()
            chart_bytes = create_chart(
                df=transformed_df,
                schema=transformed_schema,
                chart_type=chart["chart_type"],
                x_column=chart["x_column"],
                y_column=chart.get("y_column"),
                title=title,
                output_format="png",
                chart_options=chart.get("chart_options") or {},
            )
            image = plt.imread(BytesIO(chart_bytes))
            page = plt.figure(figsize=(11, 8.5))
            page.suptitle(title, fontsize=16, fontweight="bold", y=0.96)
            page.text(
                0.08,
                0.915,
                f"{chart['chart_type']} | {chart['x_column']}"
                + (f" vs {chart.get('y_column')}" if chart.get("y_column") else ""),
                fontsize=10,
                color="#4a5d75",
            )
            axis = page.add_axes([0.08, 0.2, 0.84, 0.64])
            axis.imshow(image)
            axis.axis("off")
            note = (chart.get("note") or "").strip()
            page.text(
                0.08,
                0.08,
                f"Note: {note}" if note else "Note: No note added for this chart.",
                fontsize=10,
                wrap=True,
            )
            pdf.savefig(page, bbox_inches="tight")
            plt.close(page)

    output.seek(0)
    download_name = f"{sanitize_filename_stem(filename)}_dashboard.pdf"
    return send_file(output, as_attachment=True, download_name=download_name, mimetype="application/pdf")


@app.post("/api/export/report")
def export_report() -> Any:
    payload = request.get_json(silent=True) or {}
    config = payload.get("config") or {}

    try:
        filename, clean_df, load_report, cleaning_report = _resolve_dataset(payload)
    except ValueError as error:
        return _json_response({"error": str(error)}, 404)

    transformed_df, steps = apply_transformations(clean_df, config)
    schema = detect_schema(transformed_df)
    analysis = build_analysis_report(transformed_df, schema)

    schema_counts = analysis.get("type_counts", {})
    numeric_examples = _schema_examples(schema, "numeric")
    categorical_examples = _schema_examples(schema, "categorical")
    datetime_examples = _schema_examples(schema, "datetime")
    text_examples = _schema_examples(schema, "text")
    missing_fill_count = len(cleaning_report.get("missing_values_filled", {}))
    header_fixes = load_report.get("header_fixes") or []

    report_lines = [
        "Rule-Based Data Analysis Report",
        "=" * 31,
        f"Source file: {filename}",
        f"Rows after processing: {transformed_df.shape[0]}",
        f"Columns after processing: {transformed_df.shape[1]}",
        "",
        "Executive Summary",
        "-" * 17,
        f"- Column mix: {schema_counts.get('numeric', 0)} numeric, {schema_counts.get('categorical', 0)} categorical, "
        f"{schema_counts.get('datetime', 0)} datetime, {schema_counts.get('text', 0)} text.",
        f"- Primary numeric fields: {', '.join(analysis.get('primary_numeric_fields', [])) or 'none'}.",
        f"- Categorical fields: {', '.join(analysis.get('categorical_fields', [])) or 'none'}.",
        f"- Identifier-like fields excluded from deep statistics: {', '.join(analysis.get('identifier_columns', [])) or 'none'}.",
        "",
        "Key Insights",
        "-" * 12,
    ]

    report_lines.extend(
        f"- {insight}" for insight in analysis.get("key_insights", []) or ["No standout insights were generated."]
    )
    report_lines.extend(
        [
            "",
            "Data Preparation",
            "-" * 16,
            f"- File type detected: {load_report.get('file_type', 'unknown')}",
            f"- Header fixes applied: {', '.join(header_fixes) if header_fixes else 'none'}",
            f"- Duplicate rows removed: {cleaning_report.get('duplicates_removed', 0)}",
            f"- Columns with filled missing values: {missing_fill_count}",
            f"- Type coercion applied to: {', '.join(cleaning_report.get('coerced_columns', [])) or 'none'}",
            f"- Text normalization applied to: {', '.join(cleaning_report.get('text_standardized', [])) or 'none'}",
            "",
            "Applied Transformations",
            "-" * 21,
        ]
    )

    report_lines.extend(f"- {step}" for step in steps or ["No transformation rules were applied after cleaning."])
    report_lines.extend(
        [
            "",
            "Schema Snapshot",
            "-" * 15,
            f"- Numeric examples: {', '.join(numeric_examples) or 'none'}",
            f"- Categorical examples: {', '.join(categorical_examples) or 'none'}",
            f"- Datetime examples: {', '.join(datetime_examples) or 'none'}",
            f"- Text examples: {', '.join(text_examples) or 'none'}",
            "",
            "Data Cautions",
            "-" * 13,
        ]
    )

    report_lines.extend(
        f"- {warning}" for warning in analysis.get("warnings", [])[:5]
    )
    if not analysis.get("warnings"):
        report_lines.append("- No major rule-based cautions were detected.")

    report_lines.extend(
        [
            "",
            "Top Relationships",
            "-" * 17,
        ]
    )

    top_correlations = analysis.get("top_correlations", [])[:5]
    if top_correlations:
        report_lines.extend(
            f"- {item['left']} vs {item['right']}: correlation {_format_report_value(item['value'])}"
            for item in top_correlations
        )
    else:
        report_lines.append("- Not enough numeric columns were available for correlation analysis.")

    report_lines.extend(["", "Category Comparison", "-" * 19])
    group_comparison = analysis.get("group_comparison")
    if group_comparison and group_comparison.get("largest_mean_gaps"):
        report_lines.append(f"- Compared groups in: {group_comparison['group_column']}")
        report_lines.extend(
            "- {metric}: {highest_group} ({highest_mean}) vs {lowest_group} ({lowest_mean}), difference {difference}".format(
                metric=item["metric"],
                highest_group=item["highest_group"],
                highest_mean=_format_report_value(item["highest_mean"]),
                lowest_group=item["lowest_group"],
                lowest_mean=_format_report_value(item["lowest_mean"]),
                difference=_format_report_value(item["difference"]),
            )
            for item in group_comparison["largest_mean_gaps"][:5]
        )
    else:
        report_lines.append("- No group comparison was available for this dataset.")

    report_lines.extend(["", "Sample Numeric Summary", "-" * 22])
    numeric_summary = analysis.get("numeric_summary", {})
    if numeric_summary:
        for field, stats in list(numeric_summary.items())[:6]:
            report_lines.append(
                "- {field}: mean {mean}, median {median}, min {minimum}, max {maximum}, std {std}".format(
                    field=field,
                    mean=_format_report_value(stats.get("mean")),
                    median=_format_report_value(stats.get("50%")),
                    minimum=_format_report_value(stats.get("min")),
                    maximum=_format_report_value(stats.get("max")),
                    std=_format_report_value(stats.get("std")),
                )
            )
    else:
        report_lines.append("- No numeric summary was available.")

    report_lines.extend(["", "Category Counts", "-" * 15])
    categorical_breakdown = analysis.get("categorical_breakdown", {})
    if categorical_breakdown:
        for field, values in categorical_breakdown.items():
            top_values = ", ".join(f"{label}: {count}" for label, count in list(values.items())[:6]) or "none"
            report_lines.append(f"- {field}: {top_values}")
    else:
        report_lines.append("- No categorical breakdown was available.")

    saved_charts = _normalise_chart_items(payload)
    if saved_charts:
        report_lines.extend(["", "Saved Chart Notes", "-" * 17])
        for chart in saved_charts:
            chart_name = chart.get("label") or chart.get("title") or chart.get("chart_type") or "Saved chart"
            note = (chart.get("note") or "").strip() or "No note added."
            report_lines.append(f"- {chart_name}: {note}")

    report_bytes = BytesIO("\n".join(report_lines).encode("utf-8"))
    report_name = f"{sanitize_filename_stem(filename)}_report.txt"
    return send_file(report_bytes, as_attachment=True, download_name=report_name, mimetype="text/plain")


if __name__ == "__main__":
    app.run(debug=True)
