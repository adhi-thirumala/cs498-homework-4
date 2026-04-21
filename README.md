# CS 498 Homework 4

## Files
- `clean.py` — cleans `taxi_trips.csv` → `taxi_trips_clean.csv`
- `load.py` — loads cleaned data into Neo4j
- `preprocess.py` — PySpark preprocessing (Part 2.1)
- `main.py` — FastAPI app with Neo4j + PySpark endpoints

## Part 2.1 — Run preprocessing

```bash
uv run preprocess.py
```

This writes per-company summary records to `processed_data/` as newline-delimited JSON.

### Screenshot — `processed_data/` contents and a part file

<!-- Replace this section with a screenshot after running preprocess.py -->
<!-- Suggested commands to capture:
     ls -la processed_data/
     cat processed_data/part-*.json | head
-->

![processed_data folder and part file](processed_data_screenshot.png)

## Part 2.2 — Run the API

```bash
uv run uvicorn main:app --reload
```

### Endpoints (PySpark)
- `GET /area-stats?area_id=<int>`
- `GET /top-pickup-areas?n=<int>`
- `GET /company-compare?company1=<string>&company2=<string>`
