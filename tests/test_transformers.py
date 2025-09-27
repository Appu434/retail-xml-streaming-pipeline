import os
from pyspark.sql import SparkSession
from pipeline.transformers.transactions import BronzeToSilverTransformer

def test_line_item_calc():
    spark = (SparkSession.builder
             .master("local[1]")
             .appName("test")
             .config("spark.sql.shuffle.partitions", "1")
             .getOrCreate())
    data = [{
        "order_id":"X1",
        "order_ts":"2025-01-01T00:00:00Z",
        "channel":"STORE",
        "store":{"store_id":"S1","name":"A","city":"C","state":"ST","country":"IN"},
        "customer":{"customer_id":"C1","first_name":"F","last_name":"L","email":"e","loyalty_tier":"Gold"},
        "items":{"item":[{"sku":"SKU","name":"N","qty":"2","unit_price":"10.0","discount":"1.0","tax":"0.5"}]},
        "payments":{"payment":[{"method":"CARD","amount":"19.5","currency":"INR"}]},
        "totals":{"subtotal":"20.0","total_tax":"0.5","total_discount":"1.0","grand_total":"19.5"}
    }]
    df = spark.read.json(spark.sparkContext.parallelize([str(data[0])]))
    tr = BronzeToSilverTransformer()
    outs = tr.transform(df)
    li = outs["line_items"].collect()[0]
    assert abs(li.line_subtotal - 20.0) < 1e-6
    assert abs(li.line_total - (20.0 - 1.0 + 0.5)) < 1e-6
    spark.stop()
