from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession

class SourceReader(ABC):
    @abstractmethod
    def read_stream(self, spark: SparkSession) -> DataFrame:
        ...

class Transformer(ABC):
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        ...

class SinkWriter(ABC):
    @abstractmethod
    def write_stream(self, df: DataFrame) -> None:
        ...
