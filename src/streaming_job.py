import argparse, os
from pyspark.sql import SparkSession
from pipeline.config import load_config
from pipeline.readers.xml_autoloader import XMLAutoLoaderReader
from pipeline.transformers.transactions import BronzeToSilverTransformer
from pipeline.writers.delta_writer import DeltaSinkWriter

def make_spark(env: str):
    builder = (SparkSession.builder
               .appName(f"RetailXMLStreaming-{env}")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true"))
    # Enable Delta
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")                      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return builder.getOrCreate()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["local","databricks"], default="local")
    parser.add_argument("--trigger", choices=["once","availableNow","processing_time"], default="once")
    parser.add_argument("--interval", default="10 seconds", help="for processing_time trigger")
    args = parser.parse_args()

    cfg = load_config(args.env)
    spark = make_spark(args.env)

    # Derive schema location for Auto Loader (checkpoint-like folder)
    schema_loc = os.path.join(cfg["checkpoint_base"], "schema", "bronze_transactions")
    os.makedirs(schema_loc, exist_ok=True)

    # Reader: Auto Loader XML
    reader = XMLAutoLoaderReader(
        path=cfg["raw_xml_path"],
        schema_loc=schema_loc,
        row_tag=cfg["autoloader"]["rowTag"],
        max_files_per_trigger=cfg["autoloader"]["cloudFiles.maxFilesPerTrigger"]
    )

    bronze_df = reader.read_stream(spark)

    # Write Bronze
    trigger = {"once": args.trigger=="once", "availableNow": args.trigger=="availableNow"}
    if args.trigger == "processing_time":
        trigger = {"processingTime": args.interval}

    bronze_writer = DeltaSinkWriter(
        output_path=cfg["bronze_path"],
        checkpoint_path=os.path.join(cfg["checkpoint_base"], "bronze_transactions"),
        output_mode="append",
        trigger=trigger,
        table_name=(f"{cfg['uc_catalog']}.{cfg['uc_schema']}.bronze_transactions" if cfg.get("use_unity_catalog") else None)
    )
    bronze_writer.write_stream(bronze_df)

    # Read Bronze as static for Silver step (simplifies local demo)
    bronze_static = spark.read.format("delta").load(cfg["bronze_path"])

    # Transform to Silver
    transformer = BronzeToSilverTransformer()
    outputs = transformer.transform(bronze_static)

    # Write Silver (batch for demo)
    for name, df in outputs.items():
        out_path = cfg[f"silver_{name}_path"]
        df.write.format("delta").mode("overwrite").save(out_path)

    # Create Gold
    gold_df = transformer.to_gold_daily_store(outputs["orders"], outputs["line_items"])
    gold_df.write.format("delta").mode("overwrite").save(cfg["gold_daily_store_path"])

    print("Pipeline completed for local demo. Inspect ./lakehouse/*")

if __name__ == "__main__":
    main()
