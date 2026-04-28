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
            meta = {
                "name": node.name,
                "line": node.lineno,
                "kind": "async_function" if isinstance(node, ast.AsyncFunctionDef) else "function",
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
        "functions": [
            {"name": match.group(1), "kind": "function"} for match in FUNCTION_RE.finditer(text)
        ] + [
            {"name": match.group(1), "kind": "arrow"} for match in ARROW_RE.finditer(text)
        ],
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
        "hotspots": hotspots,
        "summary": {
            "files": len(files),
            "functions": sum(len(item["functions"]) for item in files),
            "edges": len(edges),
            "folders": len({item["folder"] for item in files}),
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
    .shell {{
      display: grid;
      grid-template-columns: minmax(0, 1.45fr) minmax(320px, 440px);
      gap: 18px;
      width: min(1600px, calc(100% - 32px));
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
      grid-column: 1 / -1;
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
    h1, h2, h3, p {{ margin: 0; }}
    .hero h1 {{
      font-family: "Segoe UI Variable Display", "Bahnschrift", "Segoe UI", sans-serif;
      font-size: clamp(2rem, 3vw, 3rem);
      line-height: 0.96;
      letter-spacing: -0.04em;
      font-weight: 650;
    }}
    .hero p {{
      max-width: 85ch;
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
      min-height: 760px;
    }}
    .workspace-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: end;
      margin-bottom: 10px;
    }}
    .workspace-copy {{
      display: grid;
      gap: 8px;
    }}
    .search {{
      width: min(320px, 100%);
      padding: 12px 14px;
      border-radius: 16px;
      border: 1px solid rgba(117, 176, 255, 0.24);
      background: rgba(255,255,255,0.05);
      color: var(--text);
      outline: none;
    }}
    .search::placeholder {{ color: #8ca6c6; }}
    .graph-wrap {{
      position: relative;
      border-radius: 22px;
      overflow: hidden;
      background:
        radial-gradient(circle at center, rgba(87, 182, 255, 0.04), transparent 32%),
        linear-gradient(180deg, rgba(7, 17, 31, 0.98), rgba(6, 13, 23, 0.98));
      border: 1px solid rgba(117, 176, 255, 0.12);
      min-height: 660px;
    }}
    #graphCanvas {{
      width: 100%;
      height: 660px;
      display: block;
    }}
    .legend {{
      position: absolute;
      left: 16px;
      bottom: 16px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      max-width: 70%;
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
    .side {{
      display: grid;
      gap: 16px;
      align-content: start;
    }}
    .card {{
      padding: 18px;
      display: grid;
      gap: 12px;
    }}
    .card h2 {{
      font-family: "Segoe UI Variable Display", "Bahnschrift", "Segoe UI", sans-serif;
      font-size: 1.1rem;
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
      grid-column: 1 / -1;
      padding: 0 4px;
      color: var(--muted);
      font-size: 0.9rem;
      line-height: 1.5;
    }}
    @media (max-width: 1200px) {{
      .shell {{ grid-template-columns: 1fr; }}
      .workspace {{ min-height: 0; }}
      #graphCanvas {{ height: 620px; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero panel">
      <span class="eyebrow">Standalone repo tool</span>
      <h1>Codebase memory graph for this repository.</h1>
      <p>This is a separate development tool that maps files as stars and functions as planets. It tracks imports, shared-helper usage, page-to-page links, and endpoint relationships so we can spot workflow strands, hotspots, and duplication without reparsing the project mentally every time.</p>
      <div class="hero-stats" id="heroStats"></div>
    </section>

    <section class="workspace panel">
      <div class="workspace-head">
        <div class="workspace-copy">
          <span class="eyebrow">Constellation view</span>
          <h2>Files, functions, and cross-file strands</h2>
        </div>
        <input id="searchInput" class="search" type="text" placeholder="Search a file or function">
      </div>
      <div class="graph-wrap">
        <canvas id="graphCanvas" width="1100" height="660"></canvas>
        <div class="legend" id="legend"></div>
      </div>
    </section>

    <aside class="side">
      <section class="card panel" id="detailCard">
        <span class="eyebrow">Selection</span>
        <h2>Click a star or planet</h2>
        <p class="detail-meta">The detail panel will show file role, function satellites, inbound and outbound strands, and quick redundancy signals.</p>
        <ul class="detail-list">
          <li>Stars are files.</li>
          <li>Planets are functions or classes defined in that file.</li>
          <li>Bright strands show imports, helper usage, routes, and workflow links.</li>
        </ul>
      </section>

      <section class="card panel">
        <span class="eyebrow">Hotspots</span>
        <h2>Most connected files</h2>
        <ul class="list" id="hotspotList"></ul>
      </section>

      <section class="card panel">
        <span class="eyebrow">Possible duplication</span>
        <h2>Repeated function names</h2>
        <ul class="list" id="duplicateList"></ul>
      </section>
    </aside>

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
    const heroStats = document.getElementById("heroStats");
    const hotspotList = document.getElementById("hotspotList");
    const duplicateList = document.getElementById("duplicateList");
    const detailCard = document.getElementById("detailCard");
    const legend = document.getElementById("legend");

    const state = {{
      selectedId: null,
      search: ""
    }};

    function escapeHtml(value) {{
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
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

    function buildPositions(files) {{
      const fileNodes = [];
      const planetNodes = [];
      const hitMap = [];
      const groups = groupFilesByFolder(files);
      const cx = canvas.width / 2;
      const cy = canvas.height / 2;
      const groupRadius = Math.min(canvas.width, canvas.height) * 0.31;

      groups.forEach(([folder, groupFiles], groupIndex) => {{
        const folderAngle = (Math.PI * 2 * groupIndex) / Math.max(groups.length, 1) - Math.PI / 2;
        const folderCx = cx + Math.cos(folderAngle) * groupRadius;
        const folderCy = cy + Math.sin(folderAngle) * groupRadius;
        const orbitRadius = Math.max(54, 34 + groupFiles.length * 8);

        groupFiles.forEach((file, fileIndex) => {{
          const angle = (Math.PI * 2 * fileIndex) / Math.max(groupFiles.length, 1) - Math.PI / 2;
          const fx = groupFiles.length === 1 ? folderCx : folderCx + Math.cos(angle) * orbitRadius;
          const fy = groupFiles.length === 1 ? folderCy : folderCy + Math.sin(angle) * orbitRadius;
          const radius = Math.max(10, Math.min(18, 10 + Math.sqrt(file.functions.length || 1) + (file.degree || 0) * 0.35));

          const star = {{ ...file, kind: "file", x: fx, y: fy, r: radius }};
          fileNodes.push(star);
          hitMap.push(star);

          const orbit = radius + 20 + Math.min(42, (file.functions.length || 0) * 2.2);
          (file.functions || []).slice(0, 18).forEach((fn, fnIndex) => {{
            const pAngle = (Math.PI * 2 * fnIndex) / Math.max((file.functions || []).length, 1);
            const px = fx + Math.cos(pAngle) * orbit;
            const py = fy + Math.sin(pAngle) * orbit;
            const planet = {{
              id: `${{file.path}}::${{fn.name}}`,
              parentId: file.path,
              kind: "function",
              folder: file.folder,
              filePath: file.path,
              name: fn.name,
              label: fn.name,
              line: fn.line || null,
              functionKind: fn.kind || "function",
              x: px,
              y: py,
              r: 5
            }};
            planetNodes.push(planet);
            hitMap.push(planet);
          }});
        }});
      }});

      return {{ fileNodes, planetNodes, hitMap }};
    }}

    function attachDegrees(files, edges) {{
      const degreeMap = new Map(files.map((file) => [file.path, 0]));
      edges.forEach((edge) => {{
        degreeMap.set(edge.source, (degreeMap.get(edge.source) || 0) + 1);
        degreeMap.set(edge.target, (degreeMap.get(edge.target) || 0) + 1);
      }});
      return files.map((file) => ({{ ...file, degree: degreeMap.get(file.path) || 0 }}));
    }}

    const filesWithDegree = attachDegrees(GRAPH.files, GRAPH.edges);
    const positions = buildPositions(filesWithDegree);

    function buildLookups() {{
      return {{
        files: new Map(filesWithDegree.map((file) => [file.path, file])),
        fileNodes: new Map(positions.fileNodes.map((node) => [node.path, node])),
        functions: new Map(positions.planetNodes.map((planet) => [planet.id, planet]))
      }};
    }}

    const lookups = buildLookups();

    function isVisible(node) {{
      if (!state.search) return true;
      const haystack = `${{node.path || ""}} ${{node.name || ""}} ${{node.label || ""}}`.toLowerCase();
      return haystack.includes(state.search);
    }}

    function draw() {{
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = "#07111e";
      ctx.fillRect(0, 0, canvas.width, canvas.height);

      GRAPH.edges.forEach((edge) => {{
        const source = lookups.fileNodes.get(edge.source);
        const target = lookups.fileNodes.get(edge.target);
        if (!source || !target || !isVisible(source) || !isVisible(target)) return;
        ctx.strokeStyle = state.selectedId && (state.selectedId === source.path || state.selectedId === target.path)
          ? "rgba(110, 197, 255, 0.78)"
          : "rgba(102, 151, 215, 0.2)";
        ctx.lineWidth = state.selectedId && (state.selectedId === source.path || state.selectedId === target.path) ? 2.2 : 1;
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        const controlX = (source.x + target.x) / 2;
        const controlY = (source.y + target.y) / 2 - Math.min(90, Math.abs(source.x - target.x) * 0.12);
        ctx.quadraticCurveTo(controlX, controlY, target.x, target.y);
        ctx.stroke();
      }});

      positions.fileNodes.forEach((file) => {{
        if (!isVisible(file)) return;
        const color = FOLDER_COLORS[file.folder] || "#8aa4c8";
        ctx.strokeStyle = "rgba(255,255,255,0.08)";
        ctx.lineWidth = 1;
        if (file.functions.length) {{
          ctx.beginPath();
          ctx.arc(file.x, file.y, file.r + 20 + Math.min(42, (file.functions.length || 0) * 2.2), 0, Math.PI * 2);
          ctx.stroke();
        }}
      }});

      positions.planetNodes.forEach((planet) => {{
        const parent = lookups.fileNodes.get(planet.parentId);
        if (!parent || !isVisible(parent)) return;
        ctx.strokeStyle = state.selectedId === planet.id ? "rgba(255,255,255,0.66)" : "rgba(255,255,255,0.12)";
        ctx.beginPath();
        ctx.moveTo(parent.x, parent.y);
        ctx.lineTo(planet.x, planet.y);
        ctx.stroke();
        ctx.fillStyle = state.selectedId === planet.id ? "#ffffff" : "#d8ebff";
        ctx.beginPath();
        ctx.arc(planet.x, planet.y, planet.r, 0, Math.PI * 2);
        ctx.fill();
      }});

      positions.fileNodes.forEach((file) => {{
        if (!isVisible(file)) return;
        const color = FOLDER_COLORS[file.folder] || "#8aa4c8";
        ctx.shadowColor = color;
        ctx.shadowBlur = state.selectedId === file.path ? 28 : 16;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(file.x, file.y, file.r, 0, Math.PI * 2);
        ctx.fill();
        ctx.shadowBlur = 0;
        ctx.fillStyle = "#eef5ff";
        ctx.font = "12px Segoe UI";
        ctx.textAlign = "center";
        ctx.fillText(file.name, file.x, file.y + file.r + 16);
      }});
    }}

    function renderHeroStats() {{
      const summary = GRAPH.summary;
      heroStats.innerHTML = `
        <span class="chip">${{summary.files}} stars</span>
        <span class="chip">${{summary.functions}} planets</span>
        <span class="chip">${{summary.edges}} strands</span>
        <span class="chip">${{summary.folders}} constellations</span>
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

    function renderDuplicates() {{
      if (!GRAPH.duplicates.length) {{
        duplicateList.innerHTML = '<li>No repeated function names detected across files.</li>';
        return;
      }}
      duplicateList.innerHTML = GRAPH.duplicates.slice(0, 12).map((item) => `
        <li>
          <span class="duplicate-badge">${{escapeHtml(item.name)}}</span><br>
          <span class="muted">${{item.files.map((file) => escapeHtml(file)).join(" · ")}}</span>
        </li>
      `).join("");
    }}

    function connectedEdgesForFile(path) {{
      return GRAPH.edges.filter((edge) => edge.source === path || edge.target === path);
    }}

    function renderDetails(nodeId) {{
      if (!nodeId) {{
        detailCard.innerHTML = `
          <span class="eyebrow">Selection</span>
          <h2>Click a star or planet</h2>
          <p class="detail-meta">The detail panel will show file role, function satellites, inbound and outbound strands, and quick redundancy signals.</p>
          <ul class="detail-list">
            <li>Stars are files.</li>
            <li>Planets are functions or classes defined in that file.</li>
            <li>Bright strands show imports, helper usage, routes, and workflow links.</li>
          </ul>
        `;
        return;
      }}

      const file = lookups.files.get(nodeId);
      if (file) {{
        const related = connectedEdgesForFile(file.path);
        detailCard.innerHTML = `
          <span class="eyebrow">File star</span>
          <h2>${{escapeHtml(file.path)}}</h2>
          <p class="detail-meta">${{file.lines}} lines · ${{file.functions.length}} functions · ${{file.classes.length}} classes · ${{file.degree}} connected strands</p>
          <ul class="detail-list">
            <li><span class="detail-key">Functions</span>${{file.functions.length ? file.functions.map((fn) => escapeHtml(fn.name)).join(", ") : "No functions extracted"}}</li>
            <li><span class="detail-key">Imports / outbound links</span>${{file.imports.length ? file.imports.map((item) => escapeHtml(item)).join(", ") : "None"}}</li>
            <li><span class="detail-key">Shared helper refs</span>${{file.helper_refs.length ? file.helper_refs.map((item) => `<code>${{escapeHtml(item)}}</code>`).join(", ") : "None"}}</li>
            <li><span class="detail-key">Endpoints and pages</span>${{[...file.api_refs, ...file.page_refs].length ? [...file.api_refs, ...file.page_refs].map((item) => escapeHtml(item)).join(", ") : "None"}}</li>
            <li><span class="detail-key">Connected strands</span>${{related.length ? related.map((edge) => `${{escapeHtml(edge.source === file.path ? edge.target : edge.source)}} (${{escapeHtml(edge.labels.join(", "))}})`).join("<br>") : "No cross-file strands"}}</li>
          </ul>
        `;
        return;
      }}

      const planet = lookups.functions.get(nodeId);
      if (planet) {{
        detailCard.innerHTML = `
          <span class="eyebrow">Function planet</span>
          <h2>${{escapeHtml(planet.name)}}</h2>
          <p class="detail-meta">Defined in <code>${{escapeHtml(planet.filePath)}}</code>${{planet.line ? ` at line ${{planet.line}}` : ""}}.</p>
          <ul class="detail-list">
            <li><span class="detail-key">Kind</span>${{escapeHtml(planet.functionKind)}}</li>
            <li><span class="detail-key">Parent star</span>${{escapeHtml(planet.filePath)}}</li>
          </ul>
        `;
      }}
    }}

    function hitTest(event) {{
      const rect = canvas.getBoundingClientRect();
      const scaleX = canvas.width / rect.width;
      const scaleY = canvas.height / rect.height;
      const x = (event.clientX - rect.left) * scaleX;
      const y = (event.clientY - rect.top) * scaleY;

      const nodes = [...positions.planetNodes, ...positions.fileNodes].filter(isVisible);
      for (let index = nodes.length - 1; index >= 0; index -= 1) {{
        const node = nodes[index];
        const distance = Math.hypot(node.x - x, node.y - y);
        if (distance <= node.r + 4) {{
          return node;
        }}
      }}
      return null;
    }}

    canvas.addEventListener("click", (event) => {{
      const node = hitTest(event);
      state.selectedId = node ? (node.path || node.id) : null;
      renderDetails(state.selectedId);
      draw();
    }});

    searchInput.addEventListener("input", () => {{
      state.search = searchInput.value.trim().toLowerCase();
      draw();
    }});

    renderHeroStats();
    renderLegend();
    renderHotspots();
    renderDuplicates();
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
