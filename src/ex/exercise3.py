from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from graphframes import *

from data_loader import friends

spark = (
    SparkSession.builder.config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("FATAL")

df = spark.createDataFrame(
    data  = [
        (0, 3),
        (1, 2),
        (1, 4),
        (2, 3),
        (2, 4),
        (3, 4)
    ],
    schema = ["src", "dst"]
)
