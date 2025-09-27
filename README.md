# Retail XML Streaming Pipeline (VS Code, Databricks)

A fully modular, retail-focused XML **streaming** pipeline using **Databricks Structured Streaming**, **Auto Loader**, **Delta Lake**, **Unity Catalog**, and the **Medallion Architecture** (Raw → Bronze → Silver → Gold).

**Highlights**
- Sample **retail XML** and **schemas** included
- Modular Python package with **OOP (abstraction & inheritance)** and **decorators** (logging, timing, validation)
- Auto Loader-based **Bronze** ingestion (XML → Delta)
- **Silver** normalization and business mappings
- **Gold** aggregates (daily store sales, product KPIs)
- Unity Catalog DDL (catalog, schema, tables)
- Local demo (trigger once) and Databricks-ready jobs

---

## 1) Quick Start (Local Demo)

> This lets you run the pipeline locally to validate logic with the included sample XML. For real streaming on Databricks, see section 4.

```bash
# 1. Create & activate venv
python -m venv venv
# Windows: venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# 2. Install deps
pip install -r requirements.txt

# 3. Run a single-pass demo (reads sample XML and writes Delta to ./lakehouse)
python src/streaming_job.py --env local --trigger once
```

Outputs go to the local `./lakehouse/{bronze,silver,gold}` folders (Delta tables). You can inspect with `pyspark` or `python -i` and `spark.read.format("delta").load(...)`.

---

## 2) Project Structure

```
retail-xml-streaming-pipeline/
├── .vscode/
├── conf/
│   └── config.yaml
├── data/
│   └── raw/xml_drop/                 # landing for XML
│       ├── retail_txn_0001.xml
│       └── retail_txn_0002.xml
├── lakehouse/                        # created at runtime for local demo
├── notebooks/
│   ├── 00_setup_unity_catalog.sql
│   └── 01_create_uc_tables.sql
├── requirements.txt
├── src/
│   ├── streaming_job.py              # entrypoint
│   └── pipeline/
│       ├── __init__.py
│       ├── base.py                   # Abstract base classes
│       ├── config.py                 # YAML config loader
│       ├── schemas.py                # StructType & DDL
│       ├── utils/
│       │   ├── __init__.py
│       │   └── decorators.py         # log_time, with_logging, ensure_schema
│       ├── readers/
│       │   ├── __init__.py
│       │   └── xml_autoloader.py     # Auto Loader XML reader (inheritance)
│       ├── transformers/
│       │   ├── __init__.py
│       │   └── transactions.py       # Bronze→Silver mappings
│       └── writers/
│           ├── __init__.py
│           └── delta_writer.py       # Sink writer
├── tests/
│   └── test_transformers.py
└── scripts/
    └── dev_send_test_file.py         # copies XML to raw drop to simulate arrivals
```

---

## 3) Medallion Flow

- **Raw**: `data/raw/xml_drop/` (landing)
- **Bronze**: Parsed XML as semi-structured rows (Delta)
- **Silver**: Normalized entities (orders, line_items, payments)
- **Gold**: Aggregates (daily store sales, product KPIs)

---

## 4) Databricks Setup (Unity Catalog)

Open `notebooks/00_setup_unity_catalog.sql` and `notebooks/01_create_uc_tables.sql` in Databricks, adjust storage paths, and run them to create the catalog/schema and tables.

Then run `src/streaming_job.py` on a Databricks cluster (set `--env databricks` and update `conf/config.yaml` with your cloud locations and UC table names).

---

## 5) OOP & Decorators Overview

- **Abstraction**: `base.py` defines `SourceReader`, `Transformer`, and `SinkWriter` abstract classes.
- **Inheritance**: `XMLAutoLoaderReader(SourceReader)` implements an XML reader; you can add a `JsonAutoLoaderReader` similarly.
- **Decorators**: 
  - `@with_logging` for structured logs
  - `@log_time` for function timing
  - `@ensure_schema` to validate DataFrame schemas (useful during refactors)

---

## 6) Sample Commands

```bash
# Send another test XML into landing
python scripts/dev_send_test_file.py --id 3

# Run local once
python src/streaming_job.py --env local --trigger once

# Run "streaming" loop locally (dev only)
python src/streaming_job.py --env local --trigger processing_time --interval 10 seconds
```
