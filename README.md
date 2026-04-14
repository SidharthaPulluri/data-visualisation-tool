# Rule-Based Data Visualisation Tool

A deterministic local BI-style app for structured datasets. Upload CSV, Excel, or JSON files, run them through a fixed data pipeline, preview the cleaned/transformed table, generate standard charts, and export the results.

## Core Pipeline

`Upload -> Parse -> Detect Schema -> Clean -> Transform -> Analyse -> Visualise -> Export`

## Features

- CSV, XLSX, and JSON ingestion
- Rule-based schema detection for numeric, categorical, datetime, and text columns
- Deterministic cleaning rules for missing values, duplicates, type coercion, and text normalization
- Filtering, grouping, aggregation, and derived-column transformations
- Analysis summaries with descriptive stats and correlations
- Standard charts with type-based validation
- Export cleaned datasets and text reports

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

## Deploy

The project includes a `vercel.json` config for deploying the Flask app to Vercel.
