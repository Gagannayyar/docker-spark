from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Stream") \
        .master("local[1]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "True") \
        .getOrCreate()


