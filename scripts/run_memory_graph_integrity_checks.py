from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_codebase_memory_graph import build_graph, discover_source_files, parse_file


def _expect(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def run() -> int:
    files = [parse_file(path) for path in discover_source_files()]
    graph = build_graph(files)
    failures: list[str] = []

    file_paths = {item["path"] for item in graph["files"]}
    edges_by_source: dict[str, list[dict]] = defaultdict(list)
    for edge in graph["edges"]:
        _expect(edge["source"] in file_paths, f"missing edge source in files: {edge['source']}", failures)
        _expect(edge["target"] in file_paths, f"missing edge target in files: {edge['target']}", failures)
        _expect(bool(edge.get("reasons")), f"edge has no reasons: {edge['source']} -> {edge['target']}", failures)
        edges_by_source[edge["source"]].append(edge)
        for reason in edge.get("reasons", []):
            _expect(bool(reason.get("kind")), f"edge reason missing kind: {edge['source']} -> {edge['target']}", failures)
            _expect(bool(reason.get("detail")), f"edge reason missing detail: {edge['source']} -> {edge['target']}", failures)

    for file_info in files:
        if file_info["extension"] != ".html":
            continue

        outgoing = edges_by_source.get(file_info["path"], [])

        for style_ref in file_info.get("style_refs", []):
            matching = [
                edge
                for edge in outgoing
                if edge["target"] == style_ref
                and any(reason.get("kind") == "stylesheet-load" for reason in edge.get("reasons", []))
            ]
            _expect(
                bool(matching),
                f"missing stylesheet edge: {file_info['path']} -> {style_ref}",
                failures,
            )

        for script_ref in file_info.get("script_refs", []):
            matching = [
                edge
                for edge in outgoing
                if edge["target"] == script_ref
                and any(reason.get("kind") == "script-load" for reason in edge.get("reasons", []))
            ]
            _expect(
                bool(matching),
                f"missing script edge: {file_info['path']} -> {script_ref}",
                failures,
            )

        html_import_edges = [
            edge
            for edge in outgoing
            if any(reason.get("kind") == "import" for reason in edge.get("reasons", []))
            and edge["target"].startswith("frontend/")
        ]
        _expect(
            not html_import_edges,
            f"html file still exposes frontend asset imports as import-edges: {file_info['path']}",
            failures,
        )

    app_edges = edges_by_source.get("app.py", [])
    frontend_import_edges = [
        edge
        for edge in app_edges
        if edge["target"].startswith("frontend/")
        and any(reason.get("kind") == "import" for reason in edge.get("reasons", []))
    ]
    _expect(
        not frontend_import_edges,
        "app.py still reports import edges into frontend pages instead of serve-page edges",
        failures,
    )

    serve_page_targets = {
        edge["target"]
        for edge in app_edges
        if any(reason.get("kind") == "page-serve" for reason in edge.get("reasons", []))
    }
    for expected in {
        "frontend/upload.html",
        "frontend/prepare.html",
        "frontend/database.html",
        "frontend/guide.html",
        "frontend/visualize.html",
    }:
        _expect(expected in serve_page_targets, f"missing page-serve edge for {expected}", failures)

    styles_edges = [
        edge
        for edge in graph["edges"]
        if edge["target"] == "frontend/styles.css"
        and any(reason.get("kind") == "stylesheet-load" for reason in edge.get("reasons", []))
    ]
    _expect(bool(styles_edges), "frontend/styles.css is still isolated from stylesheet-load edges", failures)

    build_viewer_edges = [
        edge
        for edge in graph["edges"]
        if edge["source"] == "scripts/build_codebase_memory_graph.py"
        and edge["target"] == "tools/codebase_memory_graph/index.html"
        and any(reason.get("kind") == "build-artifact" for reason in edge.get("reasons", []))
    ]
    _expect(bool(build_viewer_edges), "missing build-artifact edge from generator script to standalone viewer", failures)

    print("Memory graph integrity checks complete.")
    if failures:
        print("\nFailures:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("All checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
