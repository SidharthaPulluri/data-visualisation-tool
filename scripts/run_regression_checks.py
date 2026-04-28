from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "regression" / "fixtures"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import app


def _upload_file(client: Any, fixture_path: Path) -> tuple[int, dict[str, Any]]:
    payload = {"file": (BytesIO(fixture_path.read_bytes()), fixture_path.name)}
    response = client.post("/api/upload", data=payload, content_type="multipart/form-data")
    return response.status_code, response.get_json() or {}


def _expect_equal(label: str, actual: Any, expected: Any, failures: list[str]) -> None:
    if actual != expected:
        failures.append(f"{label}: expected {expected!r}, got {actual!r}")


def _expect_in(label: str, needle: str, haystack: str | list[str], failures: list[str]) -> None:
    if isinstance(haystack, list):
        matched = any(needle in str(item) for item in haystack)
    else:
        matched = needle in str(haystack)
    if not matched:
        failures.append(f"{label}: expected to find {needle!r} in {haystack!r}")


def run() -> int:
    client = app.test_client()
    failures: list[str] = []

    for route in ["/", "/guide", "/database", "/prepare", "/visualize"]:
        response = client.get(route)
        _expect_equal(f"route {route}", response.status_code, 200, failures)

    sales_status, sales = _upload_file(client, FIXTURES / "sales_regions.csv")
    _expect_equal("sales upload", sales_status, 200, failures)
    if sales_status == 200:
        _expect_equal("sales rows", sales.get("shape", {}).get("rows"), 8, failures)
        _expect_in(
            "sales key insight",
            "Strongest relationship",
            "\n".join(sales.get("analysis", {}).get("key_insights", [])),
            failures,
        )

        transform = client.post(
            "/api/transform",
            json={
                "dataset_id": sales["dataset_id"],
                "config": {
                    "group_by": ["region"],
                    "aggregations": [{"column": "revenue", "operation": "sum"}],
                },
            },
        )
        _expect_equal("sales transform", transform.status_code, 200, failures)

        chart = client.post(
            "/api/visualize",
            json={
                "dataset_id": sales["dataset_id"],
                "chart_type": "bar",
                "x_column": "region",
                "y_column": "revenue",
                "format": "png",
                "chart_options": {"aggregation": "sum", "top_n": 4, "sort_order": "desc"},
            },
        )
        _expect_equal("sales bar chart", chart.status_code, 200, failures)

        feature_chart = client.post(
            "/api/visualize",
            json={
                "dataset_id": sales["dataset_id"],
                "chart_type": "feature_graph",
                "x_column": "revenue",
                "format": "png",
                "chart_options": {"palette": "blue"},
            },
        )
        _expect_equal("sales feature graph", feature_chart.status_code, 200, failures)
        if feature_chart.status_code == 200:
            _expect_in(
                "sales feature graph summary",
                "stronger dependencies pull nodes closer together",
                feature_chart.get_json().get("plot_data", {}).get("summary", ""),
                failures,
            )

        for label, chart_payload in [
            (
                "sales grouped bar",
                {
                    "dataset_id": sales["dataset_id"],
                    "chart_type": "grouped_bar",
                    "x_column": "region",
                    "y_column": "revenue",
                    "format": "png",
                    "chart_options": {"group_column": "quarter", "aggregation": "sum", "top_n": 4},
                },
            ),
            (
                "sales stacked bar",
                {
                    "dataset_id": sales["dataset_id"],
                    "chart_type": "stacked_bar",
                    "x_column": "region",
                    "y_column": "orders",
                    "format": "png",
                    "chart_options": {"group_column": "quarter", "aggregation": "sum", "top_n": 4},
                },
            ),
            (
                "sales bubble",
                {
                    "dataset_id": sales["dataset_id"],
                    "chart_type": "bubble",
                    "x_column": "orders",
                    "y_column": "revenue",
                    "format": "png",
                    "chart_options": {"size_column": "conversion_rate", "group_column": "quarter"},
                },
            ),
            (
                "sales density",
                {
                    "dataset_id": sales["dataset_id"],
                    "chart_type": "density",
                    "x_column": "revenue",
                    "format": "png",
                    "chart_options": {"group_column": "quarter"},
                },
            ),
            (
                "sales beeswarm",
                {
                    "dataset_id": sales["dataset_id"],
                    "chart_type": "beeswarm",
                    "x_column": "quarter",
                    "y_column": "revenue",
                    "format": "png",
                    "chart_options": {"group_column": "region", "top_n": 4},
                },
            ),
            (
                "sales hexbin",
                {
                    "dataset_id": sales["dataset_id"],
                    "chart_type": "hexbin",
                    "x_column": "orders",
                    "y_column": "revenue",
                    "format": "png",
                    "chart_options": {"bins": 12},
                },
            ),
        ]:
            response = client.post("/api/visualize", json=chart_payload)
            _expect_equal(label, response.status_code, 200, failures)

    panel_status, panel = _upload_file(client, FIXTURES / "tb_panel.tsv")
    _expect_equal("panel upload", panel_status, 200, failures)
    if panel_status == 200:
        story = panel.get("analysis", {}).get("dataset_story", {})
        _expect_equal("panel dataset intent", story.get("intent", {}).get("key"), "panel_comparison", failures)
        _expect_equal("panel focus field", story.get("focus_dimension"), "Country_or_territory_name", failures)

        line_chart = client.post(
            "/api/visualize",
            json={
                "dataset_id": panel["dataset_id"],
                "chart_type": "line",
                "x_column": "Year",
                "y_column": "Estimated_number_of_incident_cases_all_forms",
                "format": "png",
            },
        )
        _expect_equal("panel line chart", line_chart.status_code, 200, failures)

    headerless_status, headerless = _upload_file(client, FIXTURES / "house_votes_headerless.data")
    _expect_equal("headerless upload", headerless_status, 200, failures)
    if headerless_status == 200:
        _expect_equal("headerless parser mode", headerless.get("load_report", {}).get("header_mode"), "generated", failures)
        _expect_in(
            "headerless parser note",
            "Generated column names",
            headerless.get("analysis", {}).get("validation", {}).get("parser_diagnostics", []),
            failures,
        )
        headerless_feature_chart = client.post(
            "/api/visualize",
            json={
                "dataset_id": headerless["dataset_id"],
                "chart_type": "feature_graph",
                "x_column": "party",
                "format": "png",
                "chart_options": {"palette": "slate"},
            },
        )
        _expect_equal("headerless feature graph", headerless_feature_chart.status_code, 200, failures)

    summary_status, summary = _upload_file(client, FIXTURES / "summary_semicolon.csv")
    _expect_equal("summary upload", summary_status, 200, failures)
    if summary_status == 200:
        _expect_equal("summary delimiter", summary.get("load_report", {}).get("delimiter_used"), ";", failures)
        _expect_equal(
            "summary intent",
            summary.get("analysis", {}).get("dataset_story", {}).get("intent", {}).get("key"),
            "aggregated_summary_table",
            failures,
        )

    survey_status, survey = _upload_file(client, FIXTURES / "categorical_survey.json")
    _expect_equal("survey upload", survey_status, 200, failures)
    if survey_status == 200:
        _expect_equal("survey numeric count", survey.get("analysis", {}).get("type_counts", {}).get("numeric"), 0, failures)
        _expect_equal(
            "survey intent",
            survey.get("analysis", {}).get("dataset_story", {}).get("intent", {}).get("key"),
            "categorical_response_table",
            failures,
        )

    census_status, census = _upload_file(client, FIXTURES / "census_header_array.json")
    _expect_equal("census json upload", census_status, 200, failures)
    if census_status == 200:
        _expect_equal("census focus field", census.get("analysis", {}).get("dataset_story", {}).get("focus_dimension"), "NAME", failures)
        _expect_in(
            "census parser note",
            "header row followed by records",
            census.get("analysis", {}).get("validation", {}).get("parser_diagnostics", []),
            failures,
        )

    archive_status, archive = _upload_file(client, FIXTURES / "headerless_bundle.zip")
    _expect_equal("archive upload", archive_status, 200, failures)
    if archive_status == 200:
        _expect_equal("archive file type", archive.get("load_report", {}).get("file_type"), "zip", failures)
        _expect_equal("archive focus field", archive.get("analysis", {}).get("dataset_story", {}).get("focus_dimension"), "party", failures)
        _expect_equal("archive parser mode", archive.get("load_report", {}).get("header_mode"), "generated", failures)

    workbook_status, workbook = _upload_file(client, FIXTURES / "multi_sheet_workbook.xlsx")
    _expect_equal("multi-sheet workbook upload", workbook_status, 200, failures)
    if workbook_status == 200:
        _expect_equal("multi-sheet selected worksheet", workbook.get("load_report", {}).get("excel_sheet_name"), "Data", failures)
        _expect_equal("multi-sheet rows", workbook.get("shape", {}).get("rows"), 4, failures)
        _expect_in(
            "multi-sheet parser note",
            "Loaded worksheet 'Data'",
            workbook.get("analysis", {}).get("validation", {}).get("parser_diagnostics", []),
            failures,
        )

    ambiguous_workbook_status, ambiguous_workbook = _upload_file(client, FIXTURES / "ambiguous_workbook.xlsx")
    _expect_equal("ambiguous workbook upload", ambiguous_workbook_status, 200, failures)
    if ambiguous_workbook_status == 200:
        _expect_equal(
            "ambiguous workbook selected worksheet",
            ambiguous_workbook.get("load_report", {}).get("excel_sheet_name"),
            "Data Export",
            failures,
        )
        _expect_in(
            "ambiguous workbook parser note",
            "Data Export",
            ambiguous_workbook.get("analysis", {}).get("validation", {}).get("parser_diagnostics", []),
            failures,
        )

    nested_status, nested = _upload_file(client, FIXTURES / "nested_records.json")
    _expect_equal("nested json upload", nested_status, 200, failures)
    if nested_status == 200:
        _expect_equal("nested json focus field", nested.get("analysis", {}).get("dataset_story", {}).get("focus_dimension"), "country", failures)
        _expect_equal("nested json path", nested.get("load_report", {}).get("json_path"), "payload.records", failures)
        _expect_in(
            "nested json parser note",
            "payload.records",
            nested.get("analysis", {}).get("validation", {}).get("parser_diagnostics", []),
            failures,
        )

    mixed_status, mixed = _upload_file(client, FIXTURES / "mixed_schema_records.json")
    _expect_equal("mixed-schema json upload", mixed_status, 200, failures)
    if mixed_status == 200:
        _expect_equal("mixed-schema json mode", mixed.get("load_report", {}).get("json_mode"), "mixed_record_array", failures)
        _expect_equal("mixed-schema focus field", mixed.get("analysis", {}).get("dataset_story", {}).get("focus_dimension"), "country", failures)
        _expect_in(
            "mixed-schema parser note",
            "Mixed JSON array was treated as record data",
            mixed.get("analysis", {}).get("validation", {}).get("parser_diagnostics", []),
            failures,
        )

    empty_status, empty = _upload_file(client, FIXTURES / "empty.csv")
    _expect_equal("empty upload status", empty_status, 400, failures)
    _expect_in("empty upload message", "empty", empty.get("error", ""), failures)

    print("Regression checks complete.")
    if failures:
        print("\nFailures:")
        for item in failures:
            print(f"- {item}")
        return 1

    print("All checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
