from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from ..base import SourceReader
from ..schemas import xml_transaction_schema
from ..utils.decorators import with_logging, log_time

class XMLAutoLoaderReader(SourceReader):
    def __init__(self, path: str, schema_loc: str, row_tag: str = "Transaction", max_files_per_trigger: str = "100"):
        self.path = path
        self.schema_loc = schema_loc
        self.row_tag = row_tag
        self.max_files_per_trigger = max_files_per_trigger

    @with_logging
    @log_time
    def read_stream(self, spark: SparkSession) -> DataFrame:
        # Auto Loader (cloudFiles) for XML
        df = (
            spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", "xml")
                 .option("cloudFiles.schemaLocation", self.schema_loc)
                 .option("cloudFiles.maxFilesPerTrigger", self.max_files_per_trigger)
                 .option("rowTag", self.row_tag)
                 .load(self.path, schema=xml_transaction_schema)
        )
        # Attach an ingestion timestamp & source file
        df = df.withColumn("_ingest_ts", F.current_timestamp())                .withColumn("_input_file", F.input_file_name())
        return df
