from pyspark.sql.types import *

# XML schema (Transaction element)
xml_transaction_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_ts", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("store", StructType([
        StructField("store_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
    ]), True),
    StructField("customer", StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("loyalty_tier", StringType(), True),
    ]), True),
    StructField("items", StructType([
        StructField("item", ArrayType(StructType([
            StructField("sku", StringType(), True),
            StructField("name", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("unit_price", StringType(), True),
            StructField("discount", StringType(), True),
            StructField("tax", StringType(), True),
        ]))), True)
    ]), True),
    StructField("payments", StructType([
        StructField("payment", ArrayType(StructType([
            StructField("method", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("currency", StringType(), True),
        ]))), True)
    ]), True),
    StructField("totals", StructType([
        StructField("subtotal", StringType(), True),
        StructField("total_tax", StringType(), True),
        StructField("total_discount", StringType(), True),
        StructField("grand_total", StringType(), True),
    ]), True),
])

# DDL examples (useful for createOrReplaceTempView if needed)
orders_ddl = """
order_id STRING,
order_ts TIMESTAMP,
channel STRING,
store_id STRING,
store_name STRING,
store_city STRING,
store_state STRING,
store_country STRING,
customer_id STRING,
customer_first_name STRING,
customer_last_name STRING,
customer_email STRING,
customer_loyalty_tier STRING,
subtotal DOUBLE,
total_tax DOUBLE,
total_discount DOUBLE,
grand_total DOUBLE
"""

line_items_ddl = """
order_id STRING,
item_sku STRING,
item_name STRING,
qty INT,
unit_price DOUBLE,
discount DOUBLE,
tax DOUBLE,
line_subtotal DOUBLE,
line_total DOUBLE
"""

payments_ddl = """
order_id STRING,
method STRING,
amount DOUBLE,
currency STRING
"""
