from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country
import sys

print(sys.argv)

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting Apps")

    path = "pyspark-works/data/sample.csv"
    
    survey_raw_df = load_survey_df(spark=spark, data_file=path)

    partitioned_df = survey_raw_df.repartition(2)
    
    cout_df = count_by_country(partitioned_df)
    
    cout_df.show()

    input("Letit run")
    #spark.stop()

