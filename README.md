
# 🛍️ Retail XML Streaming Pipeline (Databricks + Delta Lake + Medallion)

A **retail-focused XML streaming pipeline** built with **Databricks Structured Streaming, Auto Loader, Delta Lake, Unity Catalog, and the Medallion Architecture (Raw → Bronze → Silver → Gold).**

This project demonstrates:
- **XML ingestion** with Databricks Auto Loader
- **Delta Lake** for ACID, schema evolution, and time travel
- **Unity Catalog** for secure, governed data access
- **OOP Concepts** (abstraction, inheritance, decorators)
- **End-to-End Medallion Architecture**
- **Local development** (VS Code, PySpark) + **Databricks-ready deployment**

---

## 📂 Project Structure

```
retail-xml-streaming-pipeline/
├── .vscode/                 # VS Code config (Python, Debugging)
├── conf/                    # Configurations
│   └── config.yaml
├── data/raw/xml_drop/       # Sample retail XML (landing zone)
├── lakehouse/               # Local Delta outputs (created at runtime)
├── notebooks/               # Databricks SQL setup (Unity Catalog + Tables)
├── scripts/                 # Dev helpers
├── src/                     # Modularized pipeline code
│   ├── streaming_job.py     # Entrypoint job
│   └── pipeline/
│       ├── base.py          # Abstract base classes (Abstraction)
│       ├── readers/         # XML Auto Loader (Inheritance)
│       ├── transformers/    # Bronze → Silver → Gold transformations
│       ├── writers/         # Delta sink writer
│       ├── schemas.py       # XML schema & DDLs
│       └── utils/decorators.py # Logging, timing, schema validation
└── tests/                   # PyTest unit tests
```

---

## 🏗️ Medallion Flow

1. **Raw** – XML files land in `data/raw/xml_drop/` (or ADLS/ABFSS in Databricks).
2. **Bronze** – XML ingested via **Auto Loader** into Delta.
3. **Silver** – Flatten & normalize (Orders, Line Items, Payments).
4. **Gold** – Business aggregates (e.g., Daily Store Sales).

---

## 🚀 Quick Start (Local)

```bash
# 1. Setup environment
python -m venv venv
source venv/bin/activate   # (Windows: venv\Scripts\activate)

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run local demo (single pass)
python src/streaming_job.py --env local --trigger once
```

📌 Outputs:
- Bronze → `./lakehouse/bronze/transactions`
- Silver → `./lakehouse/silver/{orders,line_items,payments}`
- Gold → `./lakehouse/gold/daily_store_sales`

Inspect:
```python
spark.read.format("delta").load("./lakehouse/silver/orders").show()
```

Simulate new XML arriving:
```bash
python scripts/dev_send_test_file.py --id 3
python src/streaming_job.py --env local --trigger once
```

---

## ☁️ Run on Databricks

1. Run `notebooks/00_setup_unity_catalog.sql` → creates catalog/schema (`retail.core`).
2. Run `notebooks/01_create_uc_tables.sql` → creates Bronze, Silver, Gold tables.
3. Update `conf/config.yaml` with **ABFSS paths** + **Unity Catalog** config.
4. Submit job:
   ```bash
   python src/streaming_job.py --env databricks --trigger availableNow
   ```

---

## 🧑‍💻 OOP Concepts Used

- **Abstraction** – `base.py` defines `SourceReader`, `Transformer`, `SinkWriter`.
- **Inheritance** – `XMLAutoLoaderReader(SourceReader)` implements XML ingestion.
- **Decorators** (`utils/decorators.py`):
  - `@with_logging` → adds structured logs
  - `@log_time` → measures execution time
  - `@ensure_schema` → validates expected schema during transformations

---

## 🔄 Data Mapping

- **Orders (Silver)** → flatten customer, store, totals.
- **Line Items (Silver)** → explode `items.item`, compute:
  ```
  line_subtotal = qty * unit_price
  line_total = line_subtotal - discount + tax
  ```
- **Payments (Silver)** → explode `payments.payment`.
- **Gold (Aggregates)**:
  - Daily revenue, discounts, taxes
  - Grouped by `store_id` and `event_date`

---

## 📊 Sample Outputs

### 🔹 Bronze Layer (semi-structured)
```text
+--------+--------------------+-------+--------------------+--------------------+
|order_id|order_ts            |channel|store               |customer            |
+--------+--------------------+-------+--------------------+--------------------+
|ORD-1001|2025-09-26T10:15:30Z|STORE  |{ST-001, Indira... }|{CUST-777, Anita...}|
...
```

### 🔸 Silver – Orders
```text
+--------+-------------------+-------+--------+-----------+----------+---------+-----------+
|order_id|order_ts           |channel|store_id|store_name |store_city|subtotal |grand_total|
+--------+-------------------+-------+--------+-----------+----------+---------+-----------+
|ORD-1001|2025-09-26 10:15:30|STORE  |ST-001  |Indira Nagar|Bengaluru|441.0    |446.0      |
|ORD-1002|2025-09-26 11:05:45|ONLINE |WEB-INDIA|Web        |Remote   |135.0    |141.08     |
```

### 🔸 Silver – Line Items
```text
+--------+--------+----------------+---+----------+--------+----+-------------+-----------+
|order_id|item_sku|item_name       |qty|unit_price|discount|tax |line_subtotal|line_total |
+--------+--------+----------------+---+----------+--------+----+-------------+-----------+
|ORD-1001|SKU-001 |Organic Apples  |2  |120.5     |10.0    |5.0 |241.0        |236.0      |
|ORD-1001|SKU-002 |Almond Milk     |1  |200.0     |0.0     |10.0|200.0        |210.0      |
|ORD-1002|SKU-003 |Whole Wheat...  |3  |45.0      |0.0     |6.08|135.0        |141.08     |
```

### 🔸 Silver – Payments
```text
+--------+------+--------+-------+
|order_id|method|amount  |currency|
+--------+------+--------+-------+
|ORD-1001|CARD  |446.0   |INR    |
|ORD-1002|UPI   |141.08  |INR    |
```

### 🟡 Gold – Daily Store Sales
```text
+----------+--------+------------+-------+---------+------+--------------+----------+-----------+
|event_date|store_id|store_city  |state  |country  |orders|gross_revenue |discounts |tax_collected|
+----------+--------+------------+-------+---------+------+--------------+----------+-----------+
|2025-09-26|ST-001  |Bengaluru   |KA     |IN       |1     |446.0         |10.0      |15.0       |
|2025-09-26|WEB-INDIA|Remote     |NA     |IN       |1     |141.08        |0.0       |6.08       |
```

---

## 🧪 Testing

Run unit tests with PyTest:
```bash
pytest tests/
```

---

## 📘 References

- [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)  
- [Delta Lake](https://delta.io/)  
- [Unity Catalog](https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/)  
- [Medallion Architecture](https://learn.microsoft.com/azure/databricks/lakehouse/medallion-architecture)

---

✨ With this project, you have a **ready-to-use retail XML pipeline** that is modular, OOP-driven, Databricks-ready, and includes **realistic sample outputs**.
