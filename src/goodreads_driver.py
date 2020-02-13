from pyspark.sql import SparkSession
from goodreads_transform import GoodreadsTransform

def create_sparksession():
    return SparkSession.builder.master('yarn').appName("goodreads") \
           .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
           .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
           .enableHiveSupport().getOrCreate()

def main():
    spark = create_sparksession()
    grt = GoodreadsTransform(spark)
    grt.transform_author_dataset()
    grt.transform_reviews_dataset()
    grt.tranform_users_dataset()
    grt.transform_books_dataset()

if __name__ == "__main__":
    main()
