from pyspark.sql.types import StringType
from pyspark.sql import functions as fn
import goodreads_udf


class GoodreadsTransform:

    def __init__(self, spark):
        self._spark = spark


    def transform_author_dataset(self):
        author_df = \
            self._spark.read.csv('s3://goodreads-working-zone/author.csv', header=True, mode='PERMISSIVE',inferSchema=True)

        author_lookup_df = author_df.groupBy('author_id')\
                            .agg(fn.max('record_create_timestamp').alias('record_create_timestamp'))
        author_lookup_df.persist()
        fn.broadcast(author_lookup_df)

        deduped_author_df = author_df\
                            .join(author_lookup_df, ['author_id', 'record_create_timestamp'], how='inner')\
                            .select(author_df.columns) \
                            .withColumn('name', goodreads_udf.remove_extra_spaces('name'))

        deduped_author_df.coalesce(3).write.csv(path='s3://goodreads-processed-zone/author/', mode='overwrite', \
                                                compression='gzip', header=True)



    def transform_reviews_dataset(self):
        reviews_df = self._spark.read \
                    .csv('s3://goodreads-working-zone/reviews.csv', header=True, \
                            mode = 'PERMISSIVE', inferSchema=True, quote = "\"", escape = "\"")

        reviews_lookup_df = reviews_df\
                            .groupBy('review_id')\
                            .agg(fn.max('record_create_timestamp').alias('record_create_timestamp'))

        reviews_lookup_df.persist()
        fn.broadcast(reviews_lookup_df)

        deduped_reviews_df = reviews_df \
                             .join(reviews_lookup_df, ['review_id', 'record_create_timestamp'], how='inner')\
                             .select(reviews_df.columns)

        deduped_reviews_df = deduped_reviews_df \
            .withColumn('review_added_date', goodreads_udf.stringtodatetime('review_added_date')) \
            .withColumn('review_updated_date', goodreads_udf.stringtodatetime('review_updated_date'))

        deduped_reviews_df.coalesce(10).write.csv(path='s3://goodreads-processed-zone/reviews/', mode='overwrite',
                                                  compression='gzip', header=True)


    def transform_books_dataset(self):
        books_df = self._spark.read.csv('s3://goodreads-working-zone/book.csv', header=True, mode='PERMISSIVE',
                                  inferSchema=True, quote="\"", escape="\"")

        books_lookup_df = books_df\
                            .groupBy('book_id')\
                            .agg(fn.max('record_create_timestamp').alias('record_create_timestamp'))
        books_lookup_df.persist()
        fn.broadcast(books_lookup_df)

        deduped_books_df = books_df\
                           .join(books_lookup_df, ['book_id', 'record_create_timestamp'], how='inner')\
                           .select(books_df.columns)
        deduped_books_df.coalesce(10).write.csv(path='s3://goodreads-processed-zone/books/', mode='overwrite', \
                                                compression='gzip', header=True)


    def tranform_users_dataset(self):
        users_df = self._spark.read.csv('s3://goodreads-working-zone/user.csv', header=True, mode='PERMISSIVE',
                                  inferSchema=True, quote="\"", escape="\"")

        users_lookup_df = users_df\
                          .groupBy('user_id')\
                           .agg(fn.max('record_create_timestamp').alias('record_create_timestamp'))

        users_lookup_df.persist()
        fn.broadcast(users_lookup_df)

        deduped_users_df = users_df\
                           .join(users_lookup_df, ['user_id', 'record_create_timestamp'], how='inner')\
                           .select(users_df.columns)

        deduped_users_df.coalesce(3).write.csv(path='s3://goodreads-processed-zone/users/', mode='overwrite',
                                               compression='gzip', header=True)
