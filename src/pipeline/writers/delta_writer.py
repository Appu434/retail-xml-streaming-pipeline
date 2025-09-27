from pyspark.sql import DataFrame
from ..base import SinkWriter
from ..utils.decorators import with_logging, log_time

class DeltaSinkWriter(SinkWriter):
    def __init__(self, output_path: str, checkpoint_path: str, output_mode: str = "append", trigger: dict | None = None, table_name: str | None = None):
        self.output_path = output_path
        self.checkpoint_path = checkpoint_path
        self.output_mode = output_mode
        self.trigger = trigger or {"once": True}
        self.table_name = table_name  # Optional UC/Delta table

    @with_logging
    @log_time
    def write_stream(self, df: DataFrame) -> None:
        writer = (df.writeStream
                    .format("delta")
                    .option("checkpointLocation", self.checkpoint_path)
                    .outputMode(self.output_mode))
        # Trigger
        if self.trigger.get("once"):
            writer = writer.trigger(once=True)
        elif self.trigger.get("availableNow"):
            writer = writer.trigger(availableNow=True)
        elif self.trigger.get("processingTime"):
            writer = writer.trigger(processingTime=self.trigger["processingTime"])

        # Optional table
        if self.table_name:
            writer = writer.option("mergeSchema", "true").toTable(self.table_name)
        else:
            writer = writer.option("path", self.output_path)

        query = writer.start()
        query.awaitTermination()
