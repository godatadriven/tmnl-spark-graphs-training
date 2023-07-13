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

g = friends(spark)

motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()

motifs.filter("b.age > 30").show()
