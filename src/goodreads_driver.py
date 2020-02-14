from pyspark.sql import SparkSession
from goodreads_transform import GoodreadsTransform
from s3_module import GoodReadsS3Module
from pathlib import Path
import logging

# Setting up logger, Logger properties are defined in logging.ini file
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

def create_sparksession():
    return SparkSession.builder.master('yarn').appName("goodreads") \
           .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
           .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
           .enableHiveSupport().getOrCreate()


def main():
    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparksession()
    grt = GoodreadsTransform(spark)

    logging.debug("\n\nCopying data from s3 landing zone to ...")
    gds3 = GoodReadsS3Module()
    gds3.s3_move_data()

    grt.transform_author_dataset()
    grt.transform_reviews_dataset()
    grt.tranform_users_dataset()
    grt.transform_books_dataset()

if __name__ == "__main__":
    main()
