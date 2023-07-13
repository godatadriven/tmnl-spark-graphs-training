import pyspark.sql.functions as f
from pyspark.sql.types import StructType, IntegerType, StringType
from graphframes import *

def friends(spark):
    """A GraphFrame of friends in a (fake) social network."""
    # Vertex DataFrame
    v = spark.createDataFrame(
        [
            ("a", "Alice", 34),
            ("b", "Bob", 36),
            ("c", "Charlie", 30),
            ("d", "David", 29),
            ("e", "Esther", 32),
            ("f", "Fanny", 36),
        ],
        ["id", "name", "age"],
    )
    # Edge DataFrame
    e = spark.createDataFrame(
        [
            ("a", "b", "friend"),
            ("b", "c", "follow"),
            ("c", "b", "follow"),
            ("f", "c", "follow"),
            ("e", "f", "follow"),
            ("e", "d", "friend"),
            ("d", "a", "friend"),
        ],
        ["src", "dst", "relationship"],
    )
    # Create a GraphFrame
    return GraphFrame(v, e)
