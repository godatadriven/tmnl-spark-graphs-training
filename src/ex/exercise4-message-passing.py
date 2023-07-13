from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from graphframes import *
from graphframes.lib import AggregateMessages as AM

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

msgToSrc = AM.dst["age"]
msgToDst = AM.src["age"]
agg = g.aggregateMessages(
    f.sum(AM.msg).alias("summedAges"),
    sendToSrc=msgToSrc,
    sendToDst=msgToDst)
agg.show()
