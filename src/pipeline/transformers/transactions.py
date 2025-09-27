from pyspark.sql import functions as F, Window
from pyspark.sql import DataFrame
from ..base import Transformer
from ..utils.decorators import with_logging, log_time, ensure_schema

class BronzeToSilverTransformer(Transformer):
    def __init__(self):
        ...

    @with_logging
    @log_time
    @ensure_schema(["order_id", "order_ts", "store", "customer", "items", "payments", "totals"])
    def _validate_bronze(self, df: DataFrame) -> DataFrame:
        return df

    @with_logging
    @log_time
    def transform(self, bronze_df: DataFrame) -> dict[str, DataFrame]:
        df = self._validate_bronze(bronze_df)

        # Orders
        orders = (
            df.select(
                F.col("order_id"),
                F.to_timestamp("order_ts").alias("order_ts"),
                F.col("channel"),
                F.col("store.store_id").alias("store_id"),
                F.col("store.name").alias("store_name"),
                F.col("store.city").alias("store_city"),
                F.col("store.state").alias("store_state"),
                F.col("store.country").alias("store_country"),
                F.col("customer.customer_id").alias("customer_id"),
                F.col("customer.first_name").alias("customer_first_name"),
                F.col("customer.last_name").alias("customer_last_name"),
                F.col("customer.email").alias("customer_email"),
                F.col("customer.loyalty_tier").alias("customer_loyalty_tier"),
                F.col("totals.subtotal").cast("double").alias("subtotal"),
                F.col("totals.total_tax").cast("double").alias("total_tax"),
                F.col("totals.total_discount").cast("double").alias("total_discount"),
                F.col("totals.grand_total").cast("double").alias("grand_total"),
                F.col("_ingest_ts"),
                F.col("_input_file")
            )
        )

        # Line items (explode array)
        items = (
            df.select("order_id", F.col("items.item").alias("item_array"))
              .withColumn("item", F.explode("item_array"))
              .select(
                  "order_id",
                  F.col("item.sku").alias("item_sku"),
                  F.col("item.name").alias("item_name"),
                  F.col("item.qty").cast("int").alias("qty"),
                  F.col("item.unit_price").cast("double").alias("unit_price"),
                  F.col("item.discount").cast("double").alias("discount"),
                  F.col("item.tax").cast("double").alias("tax"),
              )
              .withColumn("line_subtotal", F.col("qty") * F.col("unit_price"))
              .withColumn("line_total", F.col("line_subtotal") - F.col("discount") + F.col("tax"))
        )

        # Payments (explode array)
        payments = (
            df.select("order_id", F.col("payments.payment").alias("pay_array"))
              .withColumn("p", F.explode("pay_array"))
              .select(
                  "order_id",
                  F.col("p.method").alias("method"),
                  F.col("p.amount").cast("double").alias("amount"),
                  F.col("p.currency").alias("currency"),
              )
        )

        return {
            "orders": orders,
            "line_items": items,
            "payments": payments
        }

    @with_logging
    @log_time
    def to_gold_daily_store(self, orders_df: DataFrame, items_df: DataFrame) -> DataFrame:
        # Compute revenue using items to avoid trust issues with provided totals
        revenue_df = (
            items_df.groupBy("order_id")
                    .agg(F.sum("line_total").alias("order_revenue"))
        )
        enriched = (
            orders_df.join(revenue_df, "order_id", "left")
                     .withColumn("event_date", F.to_date("order_ts"))
                     .withColumn("order_revenue", F.coalesce(F.col("order_revenue"), F.col("grand_total")))
        )
        # Aggregate by store & day
        gold = (
            enriched.groupBy("event_date", "store_id", "store_city", "store_state", "store_country")
                    .agg(
                        F.countDistinct("order_id").alias("orders"),
                        F.sum("order_revenue").alias("gross_revenue"),
                        F.sum("total_discount").alias("discounts"),
                        F.sum("total_tax").alias("tax_collected")
                    )
                    .withColumn("net_revenue", F.col("gross_revenue") - F.col("discounts"))
        )
        return gold
