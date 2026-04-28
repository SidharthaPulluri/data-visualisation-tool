from __future__ import annotations

import ast
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "tools" / "codebase_memory_graph"
OUTPUT_HTML = OUTPUT_DIR / "index.html"
OUTPUT_JSON = OUTPUT_DIR / "graph-data.json"

INCLUDE_EXTENSIONS = {".py", ".js", ".html", ".css"}
SKIP_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    "exports",
    "regression",
}
SKIP_FILES_PREFIXES = ("tmp_",)
SKIP_FILES: set[str] = set()

FUNCTION_RE = re.compile(r"^\s*(?:async\s+)?function\s+([A-Za-z_]\w*)\s*\(", re.MULTILINE)
ARROW_RE = re.compile(r"^\s*(?:const|let|var)\s+([A-Za-z_]\w*)\s*=\s*(?:async\s*)?\([^)]*\)\s*=>", re.MULTILINE)
DATA_TOOL_RE = re.compile(r"window\.dataTool\.([A-Za-z_]\w*)")
API_RE = re.compile(r"['\"](/api/[^'\"#?]+)['\"]")
PAGE_RE = re.compile(r"""(?:href|window\.location\.href)\s*=\s*["'](/[^"'#?]+)["']""")
SCRIPT_SRC_RE = re.compile(r"""<script[^>]+src=["'](/static/[^"'#?]+)["']""", re.IGNORECASE)
JS_IMPORT_RE = re.compile(
    r"""^\s*import\s+(?:[^;]*?\s+from\s+)?["'](\.[^"']+)["']""",
    re.MULTILINE,
)
CSS_IMPORT_RE = re.compile(r"""@import\s+["']([^"']+)["']""")
TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{2,}")
COMMON_TOKENS = {
    "true",
    "false",
    "none",
    "null",
    "return",
    "const",
    "async",
    "await",
    "function",
    "class",
    "self",
    "this",
    "data",
    "item",
    "items",
    "value",
    "values",
    "result",
    "results",
    "list",
    "dict",
    "string",
    "number",
    "object",
    "array",
    "path",
    "file",
    "text",
    "line",
    "json",
    "html",
}
SIMILARITY_THRESHOLD = 0.52
SIMILARITY_SECONDARY_THRESHOLD = 0.42


def discover_source_files() -> list[Path]:
    files: list[Path] = []
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue
        if rel.as_posix() in SKIP_FILES:
            continue
        if path.suffix.lower() not in INCLUDE_EXTENSIONS:
            continue
        if any(path.name.startswith(prefix) for prefix in SKIP_FILES_PREFIXES):
            continue
        files.append(path)
    return sorted(files)


def safe_read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="ignore")


def top_folder(rel_path: str) -> str:
    parts = rel_path.split("/")
    if len(parts) == 1:
        return "root"
    return parts[0]


def resolve_python_module(module_name: str | None) -> str | None:
    if not module_name:
        return None
    candidate = ROOT / (module_name.replace(".", "/") + ".py")
    if candidate.exists():
        return candidate.relative_to(ROOT).as_posix()
    package_init = ROOT / module_name.replace(".", "/") / "__init__.py"
    if package_init.exists():
        return package_init.relative_to(ROOT).as_posix()
    return None


def resolve_relative_asset(source_path: Path, target: str) -> str | None:
    if target.startswith("/static/"):
        return target.replace("/static/", "frontend/")
    if not target.startswith("."):
        return None

    source_rel = source_path.relative_to(ROOT)
    resolved = (source_rel.parent / target).resolve()
    try:
        rel = resolved.relative_to(ROOT.resolve())
    except ValueError:
        return None
    if rel.exists():
        return rel.as_posix()
    if resolved.with_suffix(".js").exists():
        return resolved.with_suffix(".js").relative_to(ROOT).as_posix()
    if resolved.with_suffix(".py").exists():
        return resolved.with_suffix(".py").relative_to(ROOT).as_posix()
    return None


def split_identifier_words(value: str) -> list[str]:
    expanded = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value.replace("_", " "))
    return [part.lower() for part in expanded.split() if len(part) >= 3]


def normalize_logic_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    for raw in TOKEN_RE.findall(text):
        for token in split_identifier_words(raw):
            if token not in COMMON_TOKENS and not token.isdigit():
                tokens.append(token)
    return tokens


def build_keyword_summary(tokens: list[str], limit: int = 8) -> list[str]:
    counts: dict[str, int] = defaultdict(int)
    for token in tokens:
        counts[token] += 1
    return [
        token
        for token, _ in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]


def normalize_name_stem(name: str) -> str:
    parts = split_identifier_words(name)
    return " ".join(parts[:3]) if parts else name.lower()


def jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def extract_python_calls(function_node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    calls: list[str] = []
    for node in ast.walk(function_node):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name):
            calls.extend(split_identifier_words(func.id))
        elif isinstance(func, ast.Attribute):
            calls.extend(split_identifier_words(func.attr))
    return calls


def build_function_logic_meta(name: str, source: str, extra_tokens: list[str] | None = None) -> dict[str, Any]:
    tokens = normalize_logic_tokens(source)
    if extra_tokens:
        tokens.extend(extra_tokens)
    keywords = build_keyword_summary(tokens)
    fingerprint = sorted(set(keywords + split_identifier_words(name)[:3]))
    return {
        "keywords": keywords,
        "fingerprint": fingerprint,
        "stem": normalize_name_stem(name),
    }


def extract_braced_block(text: str, start_index: int) -> str:
    brace_start = text.find("{", start_index)
    if brace_start == -1:
        return text[start_index:text.find("\n", start_index) if text.find("\n", start_index) != -1 else len(text)]
    depth = 0
    for index in range(brace_start, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start_index:index + 1]
    return text[start_index:]


def extract_js_functions(text: str) -> list[dict[str, Any]]:
    functions: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for matcher, kind in ((FUNCTION_RE, "function"), (ARROW_RE, "arrow")):
        for match in matcher.finditer(text):
            name = match.group(1)
            line = text.count("\n", 0, match.start()) + 1
            key = (name, line)
            if key in seen:
                continue
            seen.add(key)
            snippet = extract_braced_block(text, match.start())
            logic = build_function_logic_meta(name, snippet)
            functions.append(
                {
                    "name": name,
                    "kind": kind,
                    "line": line,
                    "keywords": logic["keywords"],
                    "fingerprint": logic["fingerprint"],
                    "stem": logic["stem"],
                }
            )
    return sorted(functions, key=lambda item: (item["line"], item["name"]))


def extract_route_meta(function_node: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[str, Any]:
    paths: list[str] = []
    methods: list[str] = []
    served_file: str | None = None
    for decorator in function_node.decorator_list:
        if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute):
            attr = decorator.func.attr
            if attr in {"get", "post", "put", "delete", "patch"}:
                if decorator.args and isinstance(decorator.args[0], ast.Constant) and isinstance(decorator.args[0].value, str):
                    paths.append(decorator.args[0].value)
                    methods.append(attr.upper())
    for node in ast.walk(function_node):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "send_from_directory":
            if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str):
                served_file = f"frontend/{node.args[1].value}"
                break
    return {"paths": paths, "methods": methods, "served_file": served_file}


def parse_python(path: Path, text: str) -> dict[str, Any]:
    info: dict[str, Any] = {
        "functions": [],
        "classes": [],
        "imports": set(),
        "helper_refs": set(),
        "api_refs": set(),
        "page_refs": set(),
        "script_refs": set(),
        "routes": [],
    }
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return info

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            source = ast.get_source_segment(text, node) or ""
            logic = build_function_logic_meta(node.name, source, extract_python_calls(node))
            meta = {
                "name": node.name,
                "line": node.lineno,
                "kind": "async_function" if isinstance(node, ast.AsyncFunctionDef) else "function",
                "keywords": logic["keywords"],
                "fingerprint": logic["fingerprint"],
                "stem": logic["stem"],
            }
            route_meta = extract_route_meta(node)
            if route_meta["paths"]:
                meta["routes"] = route_meta["paths"]
                meta["methods"] = route_meta["methods"]
                if route_meta["served_file"]:
                    meta["serves"] = route_meta["served_file"]
                    info["imports"].add(route_meta["served_file"])
                info["routes"].append(meta)
            info["functions"].append(meta)
        elif isinstance(node, ast.ClassDef):
            info["classes"].append({"name": node.name, "line": node.lineno, "kind": "class"})
        elif isinstance(node, ast.Import):
            for alias in node.names:
                resolved = resolve_python_module(alias.name)
                if resolved:
                    info["imports"].add(resolved)
        elif isinstance(node, ast.ImportFrom):
            resolved = resolve_python_module(node.module)
            if resolved:
                info["imports"].add(resolved)
    return info


def parse_js_like(text: str) -> dict[str, Any]:
    return {
        "functions": extract_js_functions(text),
        "helper_refs": sorted(set(DATA_TOOL_RE.findall(text))),
        "api_refs": sorted(set(API_RE.findall(text))),
        "page_refs": sorted(set(PAGE_RE.findall(text))),
        "script_refs": [],
        "imports": sorted(set(JS_IMPORT_RE.findall(text))),
    }


def parse_html(path: Path, text: str) -> dict[str, Any]:
    info = {
        "functions": [],
        "classes": [],
        "imports": set(),
        "helper_refs": set(),
        "api_refs": set(),
        "page_refs": set(),
        "script_refs": set(),
        "routes": [],
    }
    for src in SCRIPT_SRC_RE.findall(text):
        rel = src.replace("/static/", "frontend/")
        info["imports"].add(rel)
        info["script_refs"].add(rel)

    inline_scripts = re.findall(r"<script>([\s\S]*?)</script>", text, flags=re.IGNORECASE)
    for script in inline_scripts:
        parsed = parse_js_like(script)
        info["functions"].extend(parsed["functions"])
        info["helper_refs"].update(parsed["helper_refs"])
        info["api_refs"].update(parsed["api_refs"])
        info["page_refs"].update(parsed["page_refs"])

    for href in re.findall(r"""href=["'](/[^"'#?]+)["']""", text):
        info["page_refs"].add(href)

    return info


def parse_css(path: Path, text: str) -> dict[str, Any]:
    imports = set()
    for target in CSS_IMPORT_RE.findall(text):
        resolved = resolve_relative_asset(path, target)
        if resolved:
            imports.add(resolved)
    return {
        "functions": [],
        "classes": [],
        "imports": imports,
        "helper_refs": set(),
        "api_refs": set(),
        "page_refs": set(),
        "script_refs": set(),
        "routes": [],
    }


def parse_file(path: Path) -> dict[str, Any]:
    text = safe_read(path)
    rel = path.relative_to(ROOT).as_posix()
    parser_output: dict[str, Any]
    if path.suffix == ".py":
        parser_output = parse_python(path, text)
    elif path.suffix == ".html":
        parser_output = parse_html(path, text)
    elif path.suffix == ".js":
        parser_output = parse_js_like(text) | {
            "classes": [],
            "script_refs": set(),
            "routes": [],
        }
    else:
        parser_output = parse_css(path, text)

    lines = text.count("\n") + 1 if text else 0
    functions = parser_output.get("functions", [])
    classes = parser_output.get("classes", [])
    imports = set()
    for target in parser_output.get("imports", set()):
        resolved = resolve_relative_asset(path, target) if isinstance(target, str) else None
        imports.add(resolved or target)
    return {
        "id": rel,
        "path": rel,
        "name": path.name,
        "folder": top_folder(rel),
        "extension": path.suffix.lower(),
        "lines": lines,
        "functions": functions,
        "classes": classes,
        "imports": sorted(imports),
        "helper_refs": sorted(set(parser_output.get("helper_refs", set()))),
        "api_refs": sorted(set(parser_output.get("api_refs", set()))),
        "page_refs": sorted(set(parser_output.get("page_refs", set()))),
        "script_refs": sorted(set(parser_output.get("script_refs", set()))),
        "routes": parser_output.get("routes", []),
    }


def build_route_lookup(files: dict[str, dict[str, Any]]) -> tuple[dict[str, str], dict[str, str]]:
    api_lookup: dict[str, str] = {}
    page_lookup: dict[str, str] = {}
    app_file = files.get("app.py")
    if not app_file:
        return api_lookup, page_lookup
    for route in app_file.get("routes", []):
        for path in route.get("routes", []):
            served_file = route.get("serves")
            if path.startswith("/api/"):
                api_lookup[path] = "app.py"
            elif served_file:
                page_lookup[path] = served_file
    return api_lookup, page_lookup


def add_edge(edges: dict[tuple[str, str], dict[str, Any]], source: str, target: str, label: str) -> None:
    if source == target:
        return
    key = (source, target)
    if key not in edges:
        edges[key] = {"source": source, "target": target, "labels": set(), "weight": 0}
    edges[key]["labels"].add(label)
    edges[key]["weight"] += 1


def build_similarity_clusters(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    functions: list[dict[str, Any]] = []
    for file_info in files:
        for function in file_info["functions"]:
            fingerprint = set(function.get("fingerprint", []))
            if len(fingerprint) < 3:
                continue
            functions.append(
                {
                    "id": f"{file_info['path']}::{function['name']}",
                    "file": file_info["path"],
                    "folder": file_info["folder"],
                    "name": function["name"],
                    "line": function.get("line"),
                    "kind": function.get("kind", "function"),
                    "stem": function.get("stem", normalize_name_stem(function["name"])),
                    "fingerprint": fingerprint,
                    "keywords": function.get("keywords", []),
                }
            )

    parent = list(range(len(functions)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for left in range(len(functions)):
        for right in range(left + 1, len(functions)):
            if functions[left]["file"] == functions[right]["file"]:
                continue
            score = jaccard_similarity(functions[left]["fingerprint"], functions[right]["fingerprint"])
            same_stem = functions[left]["stem"] == functions[right]["stem"]
            if score >= SIMILARITY_THRESHOLD or (same_stem and score >= SIMILARITY_SECONDARY_THRESHOLD):
                union(left, right)

    clusters: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for index, function in enumerate(functions):
        clusters[find(index)].append(function)

    similarity_clusters: list[dict[str, Any]] = []
    for cluster_functions in clusters.values():
        file_count = len({item["file"] for item in cluster_functions})
        if len(cluster_functions) < 2 or file_count < 2:
            continue
        scores: list[float] = []
        for left in range(len(cluster_functions)):
            for right in range(left + 1, len(cluster_functions)):
                scores.append(
                    jaccard_similarity(
                        cluster_functions[left]["fingerprint"],
                        cluster_functions[right]["fingerprint"],
                    )
                )
        keywords = build_keyword_summary(
            [token for item in cluster_functions for token in item["keywords"]],
            limit=6,
        )
        stems = sorted({item["stem"] for item in cluster_functions if item["stem"]})
        similarity_clusters.append(
            {
                "label": " / ".join(stems[:2]) if stems else cluster_functions[0]["name"],
                "keywords": keywords,
                "files": sorted({item["file"] for item in cluster_functions}),
                "members": [
                    {
                        "id": item["id"],
                        "file": item["file"],
                        "name": item["name"],
                        "line": item["line"],
                        "kind": item["kind"],
                    }
                    for item in sorted(cluster_functions, key=lambda member: (member["file"], member["name"]))
                ],
                "average_score": round(sum(scores) / len(scores), 3) if scores else 1.0,
                "size": len(cluster_functions),
            }
        )

    similarity_clusters.sort(
        key=lambda item: (-item["average_score"], -item["size"], item["label"])
    )
    return similarity_clusters


def build_graph(files: list[dict[str, Any]]) -> dict[str, Any]:
    by_path = {item["path"]: item for item in files}
    api_lookup, page_lookup = build_route_lookup(by_path)
    edges: dict[tuple[str, str], dict[str, Any]] = {}

    for file_info in files:
        for imported in file_info["imports"]:
            if imported in by_path:
                add_edge(edges, file_info["path"], imported, "imports")

        if file_info["helper_refs"] and "frontend/shared.js" in by_path and file_info["path"] != "frontend/shared.js":
            add_edge(edges, file_info["path"], "frontend/shared.js", "uses shared helper")

        for api in file_info["api_refs"]:
            target = api_lookup.get(api)
            if target and target in by_path:
                add_edge(edges, file_info["path"], target, api)

        for page in file_info["page_refs"]:
            target = page_lookup.get(page)
            if target and target in by_path:
                add_edge(edges, file_info["path"], target, page)

        for script_ref in file_info["script_refs"]:
            if script_ref in by_path:
                add_edge(edges, file_info["path"], script_ref, "loads script")

    degrees = defaultdict(int)
    for edge in edges.values():
        degrees[edge["source"]] += 1
        degrees[edge["target"]] += 1

    duplicate_functions: dict[str, list[str]] = defaultdict(list)
    for file_info in files:
        for function in file_info["functions"]:
            duplicate_functions[function["name"]].append(file_info["path"])

    duplicates = [
        {"name": name, "files": sorted(paths)}
        for name, paths in duplicate_functions.items()
        if len(set(paths)) > 1
    ]
    duplicates.sort(key=lambda item: (-len(item["files"]), item["name"]))

    hotspots = sorted(
        [
            {
                "path": file_info["path"],
                "degree": degrees[file_info["path"]],
                "functions": len(file_info["functions"]),
                "folder": file_info["folder"],
            }
            for file_info in files
        ],
        key=lambda item: (-item["degree"], -item["functions"], item["path"]),
    )[:10]
    similarity_clusters = build_similarity_clusters(files)

    return {
        "files": files,
        "edges": [
            {
                "source": edge["source"],
                "target": edge["target"],
                "labels": sorted(edge["labels"]),
                "weight": edge["weight"],
            }
            for edge in edges.values()
        ],
        "duplicates": duplicates,
        "similarity_clusters": similarity_clusters,
        "hotspots": hotspots,
        "summary": {
            "files": len(files),
            "functions": sum(len(item["functions"]) for item in files),
            "edges": len(edges),
            "folders": len({item["folder"] for item in files}),
            "similarity_clusters": len(similarity_clusters),
        },
        "generated_from": str(ROOT),
    }


def render_html(graph: dict[str, Any]) -> str:
    graph_json = json.dumps(graph, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Codebase Memory Graph</title>
  <style>
    :root {{
      --bg: #06111f;
      --panel: rgba(11, 24, 43, 0.88);
      --panel-strong: rgba(8, 19, 35, 0.94);
      --border: rgba(117, 176, 255, 0.18);
      --text: #eef5ff;
      --muted: #90a9c7;
      --accent: #57b6ff;
      --accent-2: #2d6cff;
      --warning: #ffbf5f;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: "Segoe UI Variable Text", "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(78, 193, 255, 0.14), transparent 22%),
        radial-gradient(circle at 78% 8%, rgba(45, 108, 255, 0.16), transparent 26%),
        linear-gradient(180deg, #081220 0%, #050c17 100%);
    }}
    h1, h2, h3, p {{ margin: 0; }}
    .shell {{
      display: grid;
      gap: 18px;
      width: min(1620px, calc(100% - 32px));
      margin: 0 auto;
      padding: 20px 0 28px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 24px;
      box-shadow: 0 18px 48px rgba(0,0,0,0.24);
      backdrop-filter: blur(10px);
    }}
    .hero {{
      padding: 22px 24px 18px;
      display: grid;
      gap: 10px;
    }}
    .eyebrow {{
      color: var(--accent);
      font-size: 0.75rem;
      font-weight: 700;
      letter-spacing: 0.18em;
      text-transform: uppercase;
    }}
    .hero h1 {{
      font-family: "Segoe UI Variable Display", "Bahnschrift", "Segoe UI", sans-serif;
      font-size: clamp(2rem, 3vw, 3rem);
      line-height: 0.96;
      letter-spacing: -0.04em;
      font-weight: 650;
    }}
    .hero p {{
      max-width: 92ch;
      color: var(--muted);
      line-height: 1.65;
    }}
    .hero-stats {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 6px;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      border: 1px solid rgba(117, 176, 255, 0.22);
      border-radius: 999px;
      background: rgba(255,255,255,0.04);
      color: #d7e7ff;
      font-size: 0.92rem;
    }}
    .workspace {{
      padding: 18px;
      overflow: hidden;
    }}
    .workspace-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: end;
      margin-bottom: 10px;
      flex-wrap: wrap;
    }}
    .workspace-copy {{
      display: grid;
      gap: 8px;
    }}
    .workspace-copy p {{
      color: var(--muted);
      max-width: 76ch;
      line-height: 1.55;
      font-size: 0.94rem;
    }}
    .controls {{
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }}
    .search {{
      width: min(340px, 100%);
      padding: 12px 14px;
      border-radius: 16px;
      border: 1px solid rgba(117, 176, 255, 0.24);
      background: rgba(255,255,255,0.05);
      color: var(--text);
      outline: none;
    }}
    .search::placeholder {{ color: #8ca6c6; }}
    .reset-button {{
      border: 1px solid rgba(117, 176, 255, 0.22);
      background: rgba(255,255,255,0.04);
      color: var(--text);
      border-radius: 16px;
      padding: 11px 14px;
      cursor: pointer;
      font: inherit;
    }}
    .graph-wrap {{
      position: relative;
      border-radius: 22px;
      overflow: hidden;
      background:
        radial-gradient(circle at center, rgba(87, 182, 255, 0.04), transparent 32%),
        linear-gradient(180deg, rgba(7, 17, 31, 0.98), rgba(6, 13, 23, 0.98));
      border: 1px solid rgba(117, 176, 255, 0.12);
      min-height: 760px;
    }}
    #graphCanvas {{
      width: 100%;
      height: 760px;
      display: block;
      cursor: default;
    }}
    .selection-pill {{
      position: absolute;
      top: 16px;
      right: 16px;
      width: min(320px, calc(100% - 32px));
      padding: 14px 16px;
      border-radius: 18px;
      background: rgba(5, 15, 28, 0.84);
      border: 1px solid rgba(117, 176, 255, 0.16);
      box-shadow: 0 12px 28px rgba(0,0,0,0.24);
      display: grid;
      gap: 4px;
      z-index: 2;
    }}
    .selection-pill h3 {{
      font-family: "Segoe UI Variable Display", "Bahnschrift", "Segoe UI", sans-serif;
      font-size: 1rem;
      letter-spacing: -0.02em;
    }}
    .selection-pill p {{
      color: var(--muted);
      font-size: 0.9rem;
      line-height: 1.45;
    }}
    .graph-help {{
      position: absolute;
      right: 16px;
      bottom: 16px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(6, 17, 31, 0.72);
      border: 1px solid rgba(117, 176, 255, 0.14);
      color: #cfe1fb;
      font-size: 0.78rem;
      z-index: 2;
    }}
    .legend {{
      position: absolute;
      left: 16px;
      bottom: 16px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      max-width: calc(100% - 260px);
      z-index: 2;
    }}
    .legend-item {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(6, 17, 31, 0.72);
      border: 1px solid rgba(117, 176, 255, 0.14);
      color: #cfe1fb;
      font-size: 0.8rem;
    }}
    .legend-swatch {{
      width: 10px;
      height: 10px;
      border-radius: 999px;
      flex: 0 0 auto;
    }}
    .bottom-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 16px;
    }}
    .detail-card {{
      grid-column: 1 / -1;
    }}
    .card {{
      padding: 18px;
      display: grid;
      gap: 12px;
    }}
    .card h2 {{
      font-family: "Segoe UI Variable Display", "Bahnschrift", "Segoe UI", sans-serif;
      font-size: 1.08rem;
      letter-spacing: -0.02em;
    }}
    .detail-meta {{
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.55;
    }}
    .list, .detail-list {{
      display: grid;
      gap: 8px;
      margin: 0;
      padding: 0;
      list-style: none;
    }}
    .list li, .detail-list li {{
      padding: 10px 12px;
      border-radius: 14px;
      background: rgba(255,255,255,0.04);
      border: 1px solid rgba(117, 176, 255, 0.12);
      color: #dce9ff;
      line-height: 1.5;
    }}
    .detail-key {{
      color: #8cb1ff;
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      margin-bottom: 4px;
      display: block;
    }}
    .muted {{
      color: var(--muted);
    }}
    code {{
      font-family: "Cascadia Code", "Consolas", monospace;
      color: #b7dbff;
    }}
    .duplicate-badge {{
      color: var(--warning);
      font-weight: 700;
    }}
    .footer-note {{
      padding: 0 4px;
      color: var(--muted);
      font-size: 0.9rem;
      line-height: 1.5;
    }}
    @media (max-width: 1120px) {{
      .bottom-grid {{
        grid-template-columns: 1fr;
      }}
    }}
    @media (max-width: 880px) {{
      .graph-wrap {{
        min-height: 640px;
      }}
      #graphCanvas {{
        height: 640px;
      }}
      .legend {{
        max-width: calc(100% - 32px);
        right: 16px;
      }}
      .graph-help {{
        display: none;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero panel">
      <span class="eyebrow">Standalone repo tool</span>
      <h1>Codebase memory graph for this repository.</h1>
      <p>This is a separate development tool that maps files as stars and functions as planets. It tracks imports, shared-helper usage, page-to-page links, endpoint relationships, and function-shape similarity so we can spot workflow strands, hotspots, and hidden duplication without reparsing the project mentally every time.</p>
      <div class="hero-stats" id="heroStats"></div>
    </section>

    <section class="workspace panel">
      <div class="workspace-head">
        <div class="workspace-copy">
          <span class="eyebrow">Constellation view</span>
          <h2>Files, functions, and cross-file strands</h2>
          <p>Folders stay separated as constellations, stars stay readable, and functions stay collapsed until a file is selected or searched so the map keeps the star-field feel without drowning in noise.</p>
        </div>
        <div class="controls">
          <input id="searchInput" class="search" type="text" placeholder="Search a file or function">
          <button id="resetViewButton" class="reset-button" type="button">Reset view</button>
        </div>
      </div>
      <div class="graph-wrap">
        <div class="selection-pill" id="selectionPill">
          <span class="eyebrow">Selection</span>
          <h3>No node selected yet</h3>
          <p>Click a star to open its functions, or click a planet to inspect a specific function.</p>
        </div>
        <canvas id="graphCanvas" width="1500" height="760"></canvas>
        <div class="legend" id="legend"></div>
        <div class="graph-help">Wheel to zoom · click a star to reveal planets · click a strand to inspect the connection</div>
      </div>
    </section>

    <section class="bottom-grid">
      <section class="card panel detail-card" id="detailCard">
        <span class="eyebrow">Selection details</span>
        <h2>Click a star or planet</h2>
        <p class="detail-meta">The detail area shows file role, function satellites, logic clusters, and linked workflow strands. It stays at the bottom so the map gets the main space.</p>
        <ul class="detail-list">
          <li>Stars are files.</li>
          <li>Planets are functions or classes defined in that file.</li>
          <li>Cross-file strands stay faint until you focus a node.</li>
        </ul>
      </section>

      <section class="card panel">
        <span class="eyebrow">Hotspots</span>
        <h2>Most connected files</h2>
        <ul class="list" id="hotspotList"></ul>
      </section>

      <section class="card panel">
        <span class="eyebrow">Logic similarity</span>
        <h2>Similar function clusters</h2>
        <ul class="list" id="similarityList"></ul>
      </section>

      <section class="card panel">
        <span class="eyebrow">Possible duplication</span>
        <h2>Repeated function names</h2>
        <ul class="list" id="duplicateList"></ul>
      </section>
    </section>

    <p class="footer-note">Generated from <code>scripts/build_codebase_memory_graph.py</code>. Re-run the generator after structural changes so the map stays useful.</p>
  </div>

  <script>
    const GRAPH = {graph_json};

    const FOLDER_COLORS = {{
      root: "#ffd166",
      frontend: "#57b6ff",
      analysis: "#7b88ff",
      cleaning: "#76d7c4",
      ingestion: "#8bc34a",
      schema: "#ff9f6e",
      transformation: "#d17dff",
      utils: "#8aa4c8",
      visualization: "#4dd0e1",
      scripts: "#ffbf5f"
    }};

    const canvas = document.getElementById("graphCanvas");
    const ctx = canvas.getContext("2d");
    const searchInput = document.getElementById("searchInput");
    const resetViewButton = document.getElementById("resetViewButton");
    const heroStats = document.getElementById("heroStats");
    const hotspotList = document.getElementById("hotspotList");
    const similarityList = document.getElementById("similarityList");
    const duplicateList = document.getElementById("duplicateList");
    const detailCard = document.getElementById("detailCard");
    const selectionPill = document.getElementById("selectionPill");
    const legend = document.getElementById("legend");

    const state = {{
      selectedId: null,
      selectedEdgeKey: null,
      search: "",
      scale: 1
    }};

    function escapeHtml(value) {{
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }}

    function attachDegrees(files, edges) {{
      const degreeMap = new Map(files.map((file) => [file.path, 0]));
      edges.forEach((edge) => {{
        degreeMap.set(edge.source, (degreeMap.get(edge.source) || 0) + 1);
        degreeMap.set(edge.target, (degreeMap.get(edge.target) || 0) + 1);
      }});
      return files.map((file) => ({{ ...file, degree: degreeMap.get(file.path) || 0 }}));
    }}

    function groupFilesByFolder(files) {{
      const groups = new Map();
      files.forEach((file) => {{
        if (!groups.has(file.folder)) {{
          groups.set(file.folder, []);
        }}
        groups.get(file.folder).push(file);
      }});
      return [...groups.entries()].sort((a, b) => a[0].localeCompare(b[0]));
    }}

    function normalizeText(value) {{
      return String(value || "").toLowerCase();
    }}

    function buildPositions(files) {{
      const fileNodes = [];
      const planetNodes = [];
      const groups = groupFilesByFolder(files);
      const marginX = 70;
      const marginY = 70;
      const cols = Math.max(2, Math.ceil(Math.sqrt(groups.length)));
      const rows = Math.max(1, Math.ceil(groups.length / cols));
      const cellWidth = (canvas.width - marginX * 2) / cols;
      const cellHeight = (canvas.height - marginY * 2) / rows;
      const groupZones = [];

      groups.forEach(([folder, groupFiles], groupIndex) => {{
        const col = groupIndex % cols;
        const row = Math.floor(groupIndex / cols);
        const x = marginX + col * cellWidth;
        const y = marginY + row * cellHeight;
        const width = cellWidth - 18;
        const height = cellHeight - 18;
        const innerPad = 54;
        const contentWidth = Math.max(140, width - innerPad * 2);
        const contentHeight = Math.max(120, height - innerPad * 2);
        const starCols = Math.max(1, Math.ceil(Math.sqrt(groupFiles.length)));
        const starRows = Math.max(1, Math.ceil(groupFiles.length / starCols));
        const stepX = contentWidth / Math.max(starCols, 1);
        const stepY = contentHeight / Math.max(starRows, 1);

        groupZones.push({{
          folder,
          x,
          y,
          width,
          height,
          centerX: x + width / 2,
          centerY: y + height / 2,
          haloRadius: Math.max(width, height) * 0.38,
          labelX: x + 18,
          labelY: y + 24
        }});

        groupFiles.forEach((file, fileIndex) => {{
          const gridCol = fileIndex % starCols;
          const gridRow = Math.floor(fileIndex / starCols);
          const fx = x + innerPad + stepX * (gridCol + 0.5);
          const fy = y + innerPad + stepY * (gridRow + 0.5);
          const radius = Math.max(10, Math.min(18, 10 + Math.sqrt(file.functions.length || 1) + (file.degree || 0) * 0.25));
          const orbit = Math.min(Math.min(stepX, stepY) * 0.42, radius + 18 + Math.min(22, (file.functions.length || 0) * 1.15));

          fileNodes.push({{ ...file, kind: "file", x: fx, y: fy, r: radius, orbit }});

          (file.functions || []).slice(0, 18).forEach((fn, fnIndex) => {{
            const pAngle = (Math.PI * 2 * fnIndex) / Math.max(Math.min((file.functions || []).length, 18), 1);
            const px = fx + Math.cos(pAngle) * orbit;
            const py = fy + Math.sin(pAngle) * orbit;
            planetNodes.push({{
              id: `${{file.path}}::${{fn.name}}`,
              parentId: file.path,
              kind: "function",
              folder: file.folder,
              filePath: file.path,
              name: fn.name,
              label: fn.name,
              line: fn.line || null,
              functionKind: fn.kind || "function",
              keywords: fn.keywords || [],
              x: px,
              y: py,
              r: 5
            }});
          }});
        }});
      }});

      return {{ fileNodes, planetNodes, groupZones }};
    }}

    const filesWithDegree = attachDegrees(GRAPH.files, GRAPH.edges);
    const positions = buildPositions(filesWithDegree);
    const fileLookup = new Map(filesWithDegree.map((file) => [file.path, file]));
    const fileNodeLookup = new Map(positions.fileNodes.map((node) => [node.path, node]));
    const functionLookup = new Map(positions.planetNodes.map((node) => [node.id, node]));
    const edgeLookup = new Map(GRAPH.edges.map((edge) => [`${{edge.source}}->${{edge.target}}`, edge]));
    const planetsByParent = new Map();
    positions.planetNodes.forEach((planet) => {{
      if (!planetsByParent.has(planet.parentId)) {{
        planetsByParent.set(planet.parentId, []);
      }}
      planetsByParent.get(planet.parentId).push(planet);
    }});

    function isFunctionMatch(planet) {{
      if (!state.search) return false;
      const haystack = `${{planet.filePath}} ${{planet.name}} ${{planet.keywords.join(" ")}}`.toLowerCase();
      return haystack.includes(state.search);
    }}

    function isVisibleFile(node) {{
      if (!state.search) return true;
      const haystack = `${{node.path}} ${{node.name}} ${{node.folder}}`.toLowerCase();
      if (haystack.includes(state.search)) return true;
      return (planetsByParent.get(node.path) || []).some(isFunctionMatch);
    }}

    function activeParentId() {{
      if (!state.selectedId) return null;
      if (fileNodeLookup.has(state.selectedId)) return state.selectedId;
      const fn = functionLookup.get(state.selectedId);
      return fn ? fn.parentId : null;
    }}

    function shouldShowPlanets(node) {{
      if (!isVisibleFile(node)) return false;
      if (state.search) return true;
      const parentId = activeParentId();
      return parentId === node.path;
    }}

    function toWorld(clientX, clientY) {{
      const rect = canvas.getBoundingClientRect();
      const scaleX = canvas.width / rect.width;
      const scaleY = canvas.height / rect.height;
      const screenX = (clientX - rect.left) * scaleX;
      const screenY = (clientY - rect.top) * scaleY;
      const translateX = (canvas.width - canvas.width * state.scale) / 2;
      const translateY = (canvas.height - canvas.height * state.scale) / 2;
      return {{
        x: (screenX - translateX) / state.scale,
        y: (screenY - translateY) / state.scale
      }};
    }}

    function resetView() {{
      state.scale = 1;
    }}

    function drawGroupZones() {{
      positions.groupZones.forEach((zone) => {{
        const color = FOLDER_COLORS[zone.folder] || "#8aa4c8";
        const gradient = ctx.createRadialGradient(zone.centerX, zone.centerY, 12, zone.centerX, zone.centerY, zone.haloRadius);
        gradient.addColorStop(0, color + "22");
        gradient.addColorStop(0.55, color + "0d");
        gradient.addColorStop(1, "rgba(0,0,0,0)");
        ctx.fillStyle = gradient;
        ctx.beginPath();
        ctx.arc(zone.centerX, zone.centerY, zone.haloRadius, 0, Math.PI * 2);
        ctx.fill();
        ctx.strokeStyle = color + "22";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.arc(zone.centerX, zone.centerY, zone.haloRadius * 0.84, 0, Math.PI * 2);
        ctx.stroke();
        ctx.fillStyle = "#dce9ff";
        ctx.font = "700 12px Segoe UI";
        ctx.textAlign = "left";
        ctx.fillText(zone.folder, zone.labelX, zone.labelY);
      }});
    }}

    function connectedEdgesForFile(path) {{
      return GRAPH.edges.filter((edge) => edge.source === path || edge.target === path);
    }}

    function edgeKey(edge) {{
      return `${{edge.source}}->${{edge.target}}`;
    }}

    function describeEdgeLabel(label) {{
      if (label === "imports") {{
        return "This file directly imports the target file, so the target provides code used by the source.";
      }}
      if (label === "uses shared helper") {{
        return "This file calls shared helpers from the target, so the target supplies reusable frontend logic.";
      }}
      if (label === "loads script") {{
        return "This page explicitly loads the target script as a runtime dependency.";
      }}
      if (label.startsWith("/api/")) {{
        return `This file calls the API route ${{label}}, which is handled in the target file.`;
      }}
      if (label.startsWith("/")) {{
        return `This file links or navigates to ${{label}}, which resolves to the target page file.`;
      }}
      return `This connection exists because of: ${{label}}.`;
    }}

    function edgeExplanation(edge) {{
      return edge.labels.map((label) => `<li><span class="detail-key">${{escapeHtml(label)}}</span>${{escapeHtml(describeEdgeLabel(label))}}</li>`).join("");
    }}

    function getVisibleEdgeGeometry(edge) {{
      const source = fileNodeLookup.get(edge.source);
      const target = fileNodeLookup.get(edge.target);
      if (!source || !target || !isVisibleFile(source) || !isVisibleFile(target)) return null;
      return {{
        edge,
        source,
        target,
        controlX: (source.x + target.x) / 2,
        controlY: (source.y + target.y) / 2 - Math.min(80, Math.abs(source.x - target.x) * 0.08)
      }};
    }}

    function pointToQuadraticDistance(point, geometry) {{
      let minDistance = Infinity;
      for (let step = 0; step <= 32; step += 1) {{
        const t = step / 32;
        const inv = 1 - t;
        const x = inv * inv * geometry.source.x + 2 * inv * t * geometry.controlX + t * t * geometry.target.x;
        const y = inv * inv * geometry.source.y + 2 * inv * t * geometry.controlY + t * t * geometry.target.y;
        minDistance = Math.min(minDistance, Math.hypot(point.x - x, point.y - y));
      }}
      return minDistance;
    }}

    function drawEdges() {{
      GRAPH.edges.forEach((edge) => {{
        const geometry = getVisibleEdgeGeometry(edge);
        if (!geometry) return;
        const key = edgeKey(edge);
        const focused = state.selectedId && (state.selectedId === geometry.source.path || state.selectedId === geometry.target.path || activeParentId() === geometry.source.path || activeParentId() === geometry.target.path);
        const selected = state.selectedEdgeKey === key;
        ctx.strokeStyle = selected
          ? "rgba(255, 211, 102, 0.92)"
          : focused
            ? "rgba(110, 197, 255, 0.72)"
            : "rgba(102, 151, 215, 0.12)";
        ctx.lineWidth = selected ? 2.6 : focused ? 1.8 : 0.8;
        ctx.beginPath();
        ctx.moveTo(geometry.source.x, geometry.source.y);
        ctx.quadraticCurveTo(geometry.controlX, geometry.controlY, geometry.target.x, geometry.target.y);
        ctx.stroke();
      }});
    }}

    function drawPlanets() {{
      positions.fileNodes.forEach((fileNode) => {{
        if (!shouldShowPlanets(fileNode)) return;
        const planets = planetsByParent.get(fileNode.path) || [];
        if (!planets.length) return;
        ctx.strokeStyle = "rgba(255,255,255,0.09)";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.arc(fileNode.x, fileNode.y, fileNode.orbit, 0, Math.PI * 2);
        ctx.stroke();

        planets.forEach((planet) => {{
          const selected = state.selectedId === planet.id;
          ctx.strokeStyle = selected ? "rgba(255,255,255,0.72)" : "rgba(255,255,255,0.10)";
          ctx.beginPath();
          ctx.moveTo(fileNode.x, fileNode.y);
          ctx.lineTo(planet.x, planet.y);
          ctx.stroke();
          ctx.fillStyle = selected ? "#ffffff" : "#d8ebff";
          ctx.beginPath();
          ctx.arc(planet.x, planet.y, planet.r, 0, Math.PI * 2);
          ctx.fill();
        }});
      }});
    }}

    function drawStars() {{
      positions.fileNodes.forEach((fileNode) => {{
        if (!isVisibleFile(fileNode)) return;
        const color = FOLDER_COLORS[fileNode.folder] || "#8aa4c8";
        const selected = state.selectedId === fileNode.path || activeParentId() === fileNode.path;
        ctx.shadowColor = color;
        ctx.shadowBlur = selected ? 30 : 14;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(fileNode.x, fileNode.y, fileNode.r, 0, Math.PI * 2);
        ctx.fill();
        ctx.shadowBlur = 0;
        ctx.fillStyle = "#eef5ff";
        ctx.font = selected ? "700 13px Segoe UI" : "12px Segoe UI";
        ctx.textAlign = "center";
        ctx.fillText(fileNode.name, fileNode.x, fileNode.y + fileNode.r + 16);
      }});
    }}

    function draw() {{
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = "#07111e";
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.save();
      ctx.translate((canvas.width - canvas.width * state.scale) / 2, (canvas.height - canvas.height * state.scale) / 2);
      ctx.scale(state.scale, state.scale);
      drawGroupZones();
      drawEdges();
      drawPlanets();
      drawStars();
      ctx.restore();
    }}

    function hitTest(event) {{
      const point = toWorld(event.clientX, event.clientY);
      const visiblePlanets = positions.planetNodes.filter((planet) => {{
        const parent = fileNodeLookup.get(planet.parentId);
        return parent && shouldShowPlanets(parent);
      }});
      const visibleStars = positions.fileNodes.filter(isVisibleFile);
      const nodes = [...visiblePlanets, ...visibleStars];
      for (let index = nodes.length - 1; index >= 0; index -= 1) {{
        const node = nodes[index];
        const distance = Math.hypot(node.x - point.x, node.y - point.y);
        if (distance <= node.r + 4) {{
          return node;
        }}
      }}
      for (let index = GRAPH.edges.length - 1; index >= 0; index -= 1) {{
        const geometry = getVisibleEdgeGeometry(GRAPH.edges[index]);
        if (!geometry) continue;
        if (pointToQuadraticDistance(point, geometry) <= 8) {{
          return {{ kind: "edge", edge: geometry.edge }};
        }}
      }}
      return null;
    }}

    function renderHeroStats() {{
      const summary = GRAPH.summary;
      heroStats.innerHTML = `
        <span class="chip">${{summary.files}} stars</span>
        <span class="chip">${{summary.functions}} planets</span>
        <span class="chip">${{summary.edges}} strands</span>
        <span class="chip">${{summary.folders}} constellations</span>
        <span class="chip">${{summary.similarity_clusters}} logic clusters</span>
      `;
    }}

    function renderLegend() {{
      const folders = [...new Set(filesWithDegree.map((file) => file.folder))].sort();
      legend.innerHTML = folders.map((folder) => `
        <span class="legend-item">
          <span class="legend-swatch" style="background:${{FOLDER_COLORS[folder] || "#8aa4c8"}}"></span>
          ${{escapeHtml(folder)}}
        </span>
      `).join("");
    }}

    function renderHotspots() {{
      hotspotList.innerHTML = GRAPH.hotspots.map((item) => `
        <li>
          <span class="detail-key">${{escapeHtml(item.folder)}}</span>
          <strong>${{escapeHtml(item.path)}}</strong><br>
          <span class="muted">${{item.degree}} strands · ${{item.functions}} functions</span>
        </li>
      `).join("");
    }}

    function renderSimilarityClusters() {{
      if (!GRAPH.similarity_clusters.length) {{
        similarityList.innerHTML = '<li>No cross-file logic clusters detected yet.</li>';
        return;
      }}
      similarityList.innerHTML = GRAPH.similarity_clusters.slice(0, 9).map((item) => `
        <li>
          <span class="detail-key">${{item.average_score.toFixed(2)}} similarity</span>
          <strong>${{escapeHtml(item.label)}}</strong><br>
          <span class="muted">${{item.size}} functions · ${{item.files.length}} files</span><br>
          <span class="muted">${{item.keywords.map((word) => escapeHtml(word)).join(" · ")}}</span>
        </li>
      `).join("");
    }}

    function renderDuplicates() {{
      if (!GRAPH.duplicates.length) {{
        duplicateList.innerHTML = '<li>No repeated function names detected across files.</li>';
        return;
      }}
      duplicateList.innerHTML = GRAPH.duplicates.slice(0, 10).map((item) => `
        <li>
          <span class="duplicate-badge">${{escapeHtml(item.name)}}</span><br>
          <span class="muted">${{item.files.map((file) => escapeHtml(file)).join(" · ")}}</span>
        </li>
      `).join("");
    }}

    function renderSelectionPill(nodeId) {{
      const edge = nodeId ? edgeLookup.get(nodeId) : null;
      if (!nodeId) {{
        selectionPill.innerHTML = `
          <span class="eyebrow">Selection</span>
          <h3>No node selected yet</h3>
          <p>Click a star to open its functions, click a planet for a specific function, or click a strand to inspect why that connection exists.</p>
        `;
        return;
      }}
      if (edge) {{
        selectionPill.innerHTML = `
          <span class="eyebrow">Connection strand</span>
          <h3>${{escapeHtml(edge.source)}} → ${{escapeHtml(edge.target)}}</h3>
          <p>${{edge.labels.length}} reason${{edge.labels.length === 1 ? "" : "s"}} recorded for this connection.</p>
        `;
        return;
      }}
      const file = fileLookup.get(nodeId);
      if (file) {{
        selectionPill.innerHTML = `
          <span class="eyebrow">File star</span>
          <h3>${{escapeHtml(file.name)}}</h3>
          <p>${{escapeHtml(file.folder)}} · ${{file.functions.length}} functions · ${{file.degree || 0}} strands</p>
        `;
        return;
      }}
      const planet = functionLookup.get(nodeId);
      if (planet) {{
        selectionPill.innerHTML = `
          <span class="eyebrow">Function planet</span>
          <h3>${{escapeHtml(planet.name)}}</h3>
          <p>${{escapeHtml(planet.filePath)}}${{planet.line ? ` · line ${{planet.line}}` : ""}}</p>
        `;
      }}
    }}

    function renderDetails(nodeId) {{
      const edge = nodeId ? edgeLookup.get(nodeId) : null;
      if (!nodeId) {{
        detailCard.innerHTML = `
          <span class="eyebrow">Selection details</span>
          <h2>Click a star or planet</h2>
          <p class="detail-meta">The detail area shows file role, function satellites, logic clusters, and linked workflow strands. It stays at the bottom so the map gets the main space.</p>
          <ul class="detail-list">
            <li>Stars are files.</li>
            <li>Planets are functions or classes defined in that file.</li>
            <li>Cross-file strands stay faint until you focus a node.</li>
          </ul>
        `;
        return;
      }}
      if (edge) {{
        detailCard.innerHTML = `
          <span class="eyebrow">Connection strand</span>
          <h2>${{escapeHtml(edge.source)}} → ${{escapeHtml(edge.target)}}</h2>
          <p class="detail-meta">This strand exists because the source file depends on or routes to the target file in one or more explicit ways.</p>
          <ul class="detail-list">
            <li><span class="detail-key">Source</span>${{escapeHtml(edge.source)}}</li>
            <li><span class="detail-key">Target</span>${{escapeHtml(edge.target)}}</li>
            ${{edgeExplanation(edge)}}
          </ul>
        `;
        return;
      }}

      const file = fileLookup.get(nodeId);
      if (file) {{
        const related = connectedEdgesForFile(file.path);
        const relatedClusters = GRAPH.similarity_clusters.filter((cluster) =>
          cluster.members.some((member) => member.file === file.path)
        );
        detailCard.innerHTML = `
          <span class="eyebrow">File star</span>
          <h2>${{escapeHtml(file.path)}}</h2>
          <p class="detail-meta">${{file.lines}} lines · ${{file.functions.length}} functions · ${{file.classes.length}} classes · ${{file.degree || 0}} connected strands</p>
          <ul class="detail-list">
            <li><span class="detail-key">Functions</span>${{file.functions.length ? file.functions.map((fn) => escapeHtml(fn.name)).join(", ") : "No functions extracted"}}</li>
            <li><span class="detail-key">Imports / outbound links</span>${{file.imports.length ? file.imports.map((item) => escapeHtml(item)).join(", ") : "None"}}</li>
            <li><span class="detail-key">Shared helper refs</span>${{file.helper_refs.length ? file.helper_refs.map((item) => `<code>${{escapeHtml(item)}}</code>`).join(", ") : "None"}}</li>
            <li><span class="detail-key">Endpoints and pages</span>${{[...file.api_refs, ...file.page_refs].length ? [...file.api_refs, ...file.page_refs].map((item) => escapeHtml(item)).join(", ") : "None"}}</li>
            <li><span class="detail-key">Logic clusters</span>${{relatedClusters.length ? relatedClusters.map((cluster) => `${{escapeHtml(cluster.label)}} [${{cluster.average_score.toFixed(2)}}]`).join("<br>") : "No strong cross-file similarity clusters"}}</li>
            <li><span class="detail-key">Connected strands</span>${{related.length ? related.map((edge) => `${{escapeHtml(edge.source === file.path ? edge.target : edge.source)}} (${{escapeHtml(edge.labels.join(", "))}})`).join("<br>") : "No cross-file strands"}}</li>
          </ul>
        `;
        return;
      }}

      const planet = functionLookup.get(nodeId);
      if (planet) {{
        const relatedCluster = GRAPH.similarity_clusters.find((cluster) =>
          cluster.members.some((member) => member.id === planet.id)
        );
        detailCard.innerHTML = `
          <span class="eyebrow">Function planet</span>
          <h2>${{escapeHtml(planet.name)}}</h2>
          <p class="detail-meta">Defined in <code>${{escapeHtml(planet.filePath)}}</code>${{planet.line ? ` at line ${{planet.line}}` : ""}}.</p>
          <ul class="detail-list">
            <li><span class="detail-key">Kind</span>${{escapeHtml(planet.functionKind)}}</li>
            <li><span class="detail-key">Parent star</span>${{escapeHtml(planet.filePath)}}</li>
            <li><span class="detail-key">Keywords</span>${{planet.keywords.length ? planet.keywords.map((keyword) => escapeHtml(keyword)).join(", ") : "None extracted"}}</li>
            <li><span class="detail-key">Logic cluster</span>${{relatedCluster ? `${{escapeHtml(relatedCluster.label)}} [${{relatedCluster.average_score.toFixed(2)}}]<br><span class="muted">${{relatedCluster.members.filter((member) => member.id !== planet.id).map((member) => `${{escapeHtml(member.file)}}::${{escapeHtml(member.name)}}`).join("<br>") || "No peers beyond this node"}}</span>` : "No strong cross-file similarity cluster"}}</li>
          </ul>
        `;
      }}
    }}

    function updateSelection(nodeId) {{
      state.selectedId = nodeId;
      state.selectedEdgeKey = nodeId && edgeLookup.has(nodeId) ? nodeId : null;
      if (state.selectedEdgeKey) {{
        state.selectedId = null;
      }}
      renderSelectionPill(nodeId);
      renderDetails(nodeId);
      draw();
    }}

    canvas.addEventListener("click", (event) => {{
      const node = hitTest(event);
      if (!node) {{
        updateSelection(null);
        return;
      }}
      if (node.kind === "edge") {{
        updateSelection(edgeKey(node.edge));
        return;
      }}
      updateSelection(node.path || node.id);
    }});

    canvas.addEventListener("wheel", (event) => {{
      event.preventDefault();
      state.scale = Math.min(2.0, Math.max(0.82, state.scale * (event.deltaY < 0 ? 1.08 : 0.92)));
      draw();
    }}, {{ passive: false }});

    searchInput.addEventListener("input", () => {{
      state.search = searchInput.value.trim().toLowerCase();
      draw();
    }});

    resetViewButton.addEventListener("click", () => {{
      resetView();
      draw();
    }});

    renderHeroStats();
    renderLegend();
    renderHotspots();
    renderSimilarityClusters();
    renderDuplicates();
    renderSelectionPill(null);
    renderDetails(null);
    draw();
  </script>
</body>
</html>
"""


def main() -> None:
    files = [parse_file(path) for path in discover_source_files()]
    graph = build_graph(files)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(graph, ensure_ascii=False, indent=2), encoding="utf-8")
    OUTPUT_HTML.write_text(render_html(graph), encoding="utf-8")
    print(f"Wrote codebase memory graph to {OUTPUT_HTML}")
    print(f"Wrote graph data to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
