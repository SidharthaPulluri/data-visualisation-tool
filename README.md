# Rule-Based Data Visualisation Tool

A deterministic local BI-style app for structured datasets. Upload one or more structured files, run them through a fixed data pipeline, inspect parser recovery and validation notes, preview the cleaned/transformed tables, generate standard charts, and export the results.

## Core Pipeline

`Upload -> Parse -> Detect Schema -> Clean -> Transform -> Analyse -> Visualise -> Export`

## Features

- Structured file ingestion for `CSV`, `TSV`, `XLSX`, `XLS`, `JSON`, ZIP archives, nested JSON payloads, multi-sheet Excel workbooks, and headerless `.data` files
- Multi-file workspaces with per-table switching in preparation and visualization
- Rule-based schema detection for numeric, categorical, datetime, and text columns
- Deterministic cleaning rules for missing values, duplicates, type coercion, and text normalization
- Parser diagnostics and recovery notes for delimiter inference, headerless fallback, duplicate-header normalization, nested JSON path selection, worksheet selection, and dropped empty rows/columns
- Validation and recovery signals for chart readiness, blocking issues, and safer fallback behavior
- Filtering, grouping, aggregation, and derived-column transformations
- Analysis summaries with descriptive stats, dataset explanation, and deterministic insight generation
- Standard charts with type-based validation and automatic recovery when a saved setup becomes invalid
- Export cleaned datasets, reports, charts, and dashboard PDFs

## Tech Stack

- Python
- Flask
- pandas
- matplotlib
- seaborn

## Run Locally

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

Open `http://127.0.0.1:5000`.

## Regression Checks

Run the core smoke-test pack to verify ingestion, transforms, intent detection, JSON header-row recovery, mixed-schema JSON arrays, nested JSON extraction, ZIP archive extraction, multi-sheet workbook selection, ambiguous workbook sheet ranking, headerless recovery, chart rendering, and empty-file handling:

```powershell
python scripts/run_regression_checks.py
```

Fixtures live in [D:\Data Visualisation Tool\regression\fixtures](D:\Data%20Visualisation%20Tool\regression\fixtures).

Run the workspace-state pack to verify multi-file session persistence, active-table restoration, per-table chart state preservation, and saved-workspace reopen flows:

```powershell
node scripts/run_workspace_regression_checks.js
```

## Memory Graph

Use [D:\Data Visualisation Tool\MEMORY_GRAPH.md](D:\Data%20Visualisation%20Tool\MEMORY_GRAPH.md) as the living maintenance map for active flows, shared state, cleanup decisions, and known residuals.

To build the standalone codebase memory graph tool that maps files as stars and functions as planets:

```powershell
python scripts/build_codebase_memory_graph.py
```

Generated outputs:

- [D:\Data Visualisation Tool\tools\codebase_memory_graph\index.html](D:\Data%20Visualisation%20Tool\tools\codebase_memory_graph\index.html)
- [D:\Data Visualisation Tool\tools\codebase_memory_graph\graph-data.json](D:\Data%20Visualisation%20Tool\tools\codebase_memory_graph\graph-data.json)

## Deploy

The project includes a `vercel.json` config for deploying the Flask app to Vercel.
