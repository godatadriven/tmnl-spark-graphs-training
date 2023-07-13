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
spark.sparkContext.setCheckpointDir("../checkpoints")

g = friends(spark)

result = g.stronglyConnectedComponents(maxIter=10)
result.select("id", "component").orderBy("component").show()
