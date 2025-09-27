from pyspark.sql import SparkSession


def make_spark(env: str):
    """
    Create and configure a SparkSession for local or databricks env.
    """
    builder = (
        SparkSession.builder
        .appName(f"RetailXMLStreaming-{env}")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.hadoop.io.nativeio.native.lib.available", "false")  # Disable native IO on Windows
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.4.0,com.databricks:spark-xml_2.12:0.16.0"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    return builder.getOrCreate()
