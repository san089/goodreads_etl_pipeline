class AnalyticsQueries:

    create_schema = """CREATE SCHEMA IF NOT EXISTS goodreads_analytics;"""

    create_author_reviews = """
        CREATE TABLE IF NOT EXISTS goodreads_analytics.popular_authors_review_count
        (
            author_id BIGINT PRIMARY KEY DISTKEY,
            review_count BIGINT,
            name VARCHAR,
            role VARCHAR,
            profile_url VARCHAR,
            average_rating FLOAT,
            rating_count INT,
            text_review_count INT,
            record_create_timestamp TIMESTAMP
        )
        ;
        """

    create_author_rating = """
        CREATE TABLE IF NOT EXISTS goodreads_analytics.popular_authors_average_rating
        (
            author_id BIGINT PRIMARY KEY DISTKEY,
            average_review_rating FLOAT,
            name VARCHAR,
            role VARCHAR,
            profile_url VARCHAR,
            average_rating FLOAT,
            rating_count INT,
            text_review_count INT,
            record_create_timestamp TIMESTAMP
        )
        ;
    """

    create_best_authors = """
        CREATE TABLE IF NOT EXISTS goodreads_analytics.best_authors
        (
            author_id BIGINT PRIMARY KEY DISTKEY,
            review_count BIGINT,
            average_review_rating FLOAT,
            name VARCHAR,
            role VARCHAR,
            profile_url VARCHAR,
            average_rating FLOAT,
            rating_count INT,
            text_review_count INT,
            record_create_timestamp TIMESTAMP
        )
        ;
    """

    populate_authors_reviews = """
        INSERT INTO goodreads_analytics.popular_authors_review_count
        SELECT a.author_id as author_id, review_count, name, role, profile_url, average_rating, rating_count, text_review_count, record_create_timestamp
        FROM (
        SELECT TOP 10 re.author_id as author_id, count(re.review_id) as review_count
        FROM
        goodreads_warehouse.reviews as re
        where re.record_create_timestamp > '{0}' and re.record_create_timestamp < '{1}'
        group by re.author_id
        order by review_count desc
        ) a
        inner join goodreads_warehouse.authors b
        ON a.author_id = b.author_id
        ;
    """

    populate_authors_ratings = """
        INSERT INTO goodreads_analytics.popular_authors_average_rating
        SELECT a.author_id as author_id, average_review_rating, name, role, profile_url, average_rating, rating_count, text_review_count, record_create_timestamp
        FROM (
        SELECT TOP 10 re.author_id as author_id, avg(re.review_rating) as average_review_rating
        FROM
        goodreads_warehouse.reviews as re
        where re.record_create_timestamp > '{0}' and re.record_create_timestamp < '{1}'
        group by re.author_id
        order by average_review_rating desc
        ) a
        inner join goodreads_warehouse.authors b
        ON a.author_id = b.author_id
        ;
    """

    populate_best_authors = """
        INSERT INTO goodreads_analytics.best_authors
        SELECT ar.author_id, rc.review_count, ar.average_review_rating, ar.name, ar.role, ar.profile_url, ar.average_rating, ar.rating_count, ar.text_review_count, ar.record_create_timestamp
        FROM goodreads_analytics.popular_authors_average_rating ar
        INNER JOIN
        goodreads_analytics.popular_authors_review_count rc
        ON ar.author_id = rc.author_id;
    """