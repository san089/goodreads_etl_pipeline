create_staging_schema = "CREATE SCHEMA IF NOT EXISTS goodreads_staging;"

drop_authors_table = "DROP TABLE IF EXISTS goodreads_staging.authors;"
drop_reviews_table = "DROP TABLE IF EXISTS goodreads_staging.reviews;"
drop_books_table = "DROP TABLE IF EXISTS goodreads_staging.books;"
drop_users_table = "DROP TABLE IF EXISTS goodreads_staging.users;"

create_authors_table = """
CREATE TABLE IF NOT EXISTS goodreads_staging.authors
(
    author_id BIGINT PRIMARY KEY,
    name VARCHAR,
    role VARCHAR,
    profile_url VARCHAR,
    average_rating FLOAT,
    rating_count INT,
    text_review_count INT,
    record_create_timestamp TIMESTAMP
)
DISTSTYLE ALL
;
"""

create_reviews_table = """
CREATE TABLE IF NOT EXISTS goodreads_staging.reviews
(
    review_id BIGINT PRIMARY KEY ,
    user_id BIGINT,
    book_id BIGINT,
    author_id BIGINT,
    review_text VARCHAR(max),
    review_rating FLOAT,
    review_votes INT,
    spoiler_flag BOOLEAN,
    spoiler_state VARCHAR,
    review_added_date TIMESTAMP,
    review_updated_date TIMESTAMP,
    review_read_count INT,
    comments_count INT,
    review_url VARCHAR,
    record_create_timestamp TIMESTAMP
)
DISTSTYLE ALL
;
"""

create_books_table = """
CREATE TABLE IF NOT EXISTS goodreads_staging.books
(
    book_id BIGINT PRIMARY KEY ,
    title VARCHAR,
    title_without_series VARCHAR,
    image_url VARCHAR,
    book_url VARCHAR,
    num_pages INT,
    "format" VARCHAR,
    edition_information VARCHAR,
    publisher VARCHAR,
    publication_day INT2,
    publication_year INT2,
    publication_month INT2,
    average_rating FLOAT,
    ratings_count INT,
    description VARCHAR(max),
    authors BIGINT,
    published INT2,
    record_create_timestamp TIMESTAMP
)
DISTSTYLE ALL
;
"""

create_users_table = """
CREATE TABLE IF NOT EXISTS goodreads_staging.users
(
    user_id BIGINT PRIMARY KEY ,
    user_name VARCHAR,
    user_display_name VARCHAR,
    location VARCHAR,
    profile_link VARCHAR,
    uri VARCHAR,
    user_image_url VARCHAR,
    small_image_url VARCHAR,
    has_image BOOLEAN,
    record_create_timestamp TIMESTAMP
)
DISTSTYLE ALL
;
"""

copy_authors_table = """
COPY goodreads_staging.authors
FROM 's3://goodread-processed-zone/authors'
IAM_ROLE 'arn:aws:iam::355886286429:role/Redshift_IAM_ROLE'
CSV
DELIMITER '|'
GZIP
NULL AS  '\000'
IGNOREHEADER 1
;
"""


copy_reviews_table = """
COPY goodreads_staging.reviews
FROM 's3://goodread-processed-zone/reviews'
IAM_ROLE 'arn:aws:iam::355886286429:role/Redshift_IAM_ROLE'
CSV
DELIMITER '|'
GZIP
NULL AS  '\000'
IGNOREHEADER 1
;
"""

copy_books_table = """
COPY goodreads_staging.books
FROM 's3://goodread-processed-zone/books'
IAM_ROLE 'arn:aws:iam::355886286429:role/Redshift_IAM_ROLE'
CSV
DELIMITER '|'
GZIP
NULL AS '\000'
IGNOREHEADER 1
;
"""


copy_users_table = """
COPY goodreads_staging.users
FROM 's3://goodread-processed-zone/users'
IAM_ROLE 'arn:aws:iam::355886286429:role/Redshift_IAM_ROLE'
CSV
DELIMITER '|'
GZIP
NULL AS '\000'
IGNOREHEADER 1
;
"""

drop_staging_tables = [drop_authors_table, drop_reviews_table, drop_books_table, drop_users_table]
create_staging_tables = [create_authors_table, create_reviews_table, create_books_table, create_users_table]
copy_staging_tables = [copy_authors_table, copy_reviews_table, copy_books_table, copy_users_table]