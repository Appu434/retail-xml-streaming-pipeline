from pyspark.sql import DataFrame, functions as F


class RetailTransformer:
    """
    Handles transformations from Bronze XML into Silver (orders, line_items, payments)
    and Gold (daily store sales aggregations).
    """

    def transform(self, bronze_df: DataFrame):
        """
        Splits the Bronze DataFrame (raw XML transactions) into
        Orders, LineItems, and Payments Silver-level DataFrames.
        """

        # ------------------------
        # Orders
        # ------------------------
        orders_df = (
            bronze_df
            .select(
                F.col("TransactionID").alias("order_id"),
                F.col("StoreID").alias("store_id"),
                F.col("TransactionDate").alias("order_date"),
                F.col("CustomerID").alias("customer_id")
            )
            .dropDuplicates(["order_id"])
        )

        # ------------------------
        # Line Items
        # ------------------------
        line_items_df = (
            bronze_df
            .withColumn("line_item", F.explode(F.col("Items.Item")))
            .select(
                F.col("TransactionID").alias("order_id"),
                F.col("line_item.SKU").alias("sku"),
                F.col("line_item.Quantity").cast("int").alias("quantity"),
                F.col("line_item.Price").cast("double").alias("price")
            )
        )

        # ------------------------
        # Payments
        # ------------------------
        payments_df = (
            bronze_df
            .withColumn("payment", F.explode(F.col("Payments.Payment")))
            .select(
                F.col("TransactionID").alias("order_id"),
                F.col("payment.Method").alias("method"),
                F.col("payment.Amount").cast("double").alias("amount")
            )
        )

        return orders_df, line_items_df, payments_df

    def aggregate_daily_sales(self, orders_df: DataFrame, line_items_df: DataFrame, payments_df: DataFrame) -> DataFrame:
        """
        Aggregates Silver-level data into Gold daily store sales.
        """

        # Join Orders with Line Items
        sales_df = (
            orders_df
            .join(line_items_df, "order_id")
            .withColumn("sales_amount", F.col("quantity") * F.col("price"))
        )

        # Aggregate: total sales per store per day
        gold_df = (
            sales_df
            .groupBy("store_id", "order_date")
            .agg(
                F.sum("sales_amount").alias("total_sales"),
                F.countDistinct("order_id").alias("num_orders"),
                F.sum("quantity").alias("total_items_sold")
            )
        )

        return gold_df
