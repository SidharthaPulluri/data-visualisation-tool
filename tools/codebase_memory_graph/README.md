# Codebase Memory Graph

This is a separate developer tool for understanding the repository.

## What it maps

- `stars` = source files
- `planets` = functions inside those files
- `strands` = imports, shared-helper usage, page-to-page links, script loading, and API workflow connections

The goal is to help us:

- see file relationships without reopening every file
- spot hotspots and strongly connected workflow files
- notice repeated function names and partial duplication
- keep a cached mental model of the repo after structural changes

## Build it

From the project root:

```powershell
python scripts/build_codebase_memory_graph.py
```

That generates:

- [D:\Data Visualisation Tool\tools\codebase_memory_graph\index.html](D:\Data%20Visualisation%20Tool\tools\codebase_memory_graph\index.html)
- [D:\Data Visualisation Tool\tools\codebase_memory_graph\graph-data.json](D:\Data%20Visualisation%20Tool\tools\codebase_memory_graph\graph-data.json)

## Use it

Open `index.html` directly in a browser. The view is static after generation, so you can inspect the graph repeatedly without rescanning the codebase each time.

## When to rebuild

Re-run the generator after:

- adding or deleting source files
- extracting shared helpers
- moving logic between frontend pages and shared modules
- changing API routes or route-to-page wiring
- major cleanup or deduplication passes
