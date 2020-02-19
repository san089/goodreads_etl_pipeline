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

    # BOOKS

    create_book_reviews = """
        CREATE TABLE IF NOT EXISTS goodreads_analytics.popular_books_review_count
        (
            book_id BIGINT PRIMARY KEY,
            review_count BIGINT,
            title VARCHAR,
            title_without_series VARCHAR,
            image_url VARCHAR,
            book_url VARCHAR,
            num_pages INT,
            "format" VARCHAR,
            edition_information VARCHAR,
            publisher VARCHAR,
            average_rating FLOAT,
            ratings_count INT,
            description VARCHAR(max),
            authors BIGINT,
            record_create_timestamp TIMESTAMP
        )
        ;
    """

    create_book_rating = """
        CREATE TABLE IF NOT EXISTS goodreads_analytics.popular_books_average_rating
        (
            book_id BIGINT PRIMARY KEY,
            average_reviews_rating FLOAT,
            title VARCHAR,
            title_without_series VARCHAR,
            image_url VARCHAR,
            book_url VARCHAR,
            num_pages INT,
            "format" VARCHAR,
            edition_information VARCHAR,
            publisher VARCHAR,
            average_rating FLOAT,
            ratings_count INT,
            description VARCHAR(max),
            authors BIGINT,
            record_create_timestamp TIMESTAMP
        )
        ;
    """

    create_best_books = """
        CREATE TABLE IF NOT EXISTS goodreads_analytics.best_books
        (
            book_id BIGINT PRIMARY KEY,
            average_reviews_rating BIGINT,
            review_count BIGINT,
            title VARCHAR,
            title_without_series VARCHAR,
            image_url VARCHAR,
            book_url VARCHAR,
            num_pages INT,
            "format" VARCHAR,
            edition_information VARCHAR,
            publisher VARCHAR,
            average_rating FLOAT,
            ratings_count INT,
            description VARCHAR(max),
            authors BIGINT,
            record_create_timestamp TIMESTAMP
        )
        ;
    """


    populate_books_reviews = """
        INSERT INTO goodreads_analytics.popular_books_review_count
        SELECT a.book_id as author_id, review_count, title, title_without_series, image_url, book_url, num_pages, format,
               edition_information, publisher, average_rating, ratings_count, description, authors, record_create_timestamp
        FROM (
        SELECT TOP 10 re.book_id as book_id, count(re.review_id) as review_count
        FROM
        goodreads_warehouse.reviews as re
        where re.record_create_timestamp > '{0}' and re.record_create_timestamp < '{1}'
        group by re.book_id
        order by review_count desc
        ) a
        inner join goodreads_warehouse.books b
        ON a.book_id = b.book_id
        order by review_count
        ;
    """


    populate_books_ratings = """
        INSERT INTO goodreads_analytics.popular_books_average_rating
        SELECT a.book_id as author_id, average_review_rating, title, title_without_series, image_url, book_url, num_pages, format,
               edition_information, publisher, average_rating, ratings_count, description, authors, record_create_timestamp
        FROM (
        SELECT TOP 10 re.book_id as book_id, avg(re.review_rating) as average_review_rating
        FROM
        goodreads_warehouse.reviews as re
        where re.record_create_timestamp > '{0}' and re.record_create_timestamp < '{1}'
        group by re.book_id
        order by average_review_rating desc
        ) a
        inner join goodreads_warehouse.books b
        ON a.book_id = b.book_id
        order by average_review_rating
        ;
    """

    populate_best_books = """
        INSERT INTO goodreads_analytics.best_books
        SELECT ar.book_id, ar.average_reviews_rating, rc.review_count, ar.title, ar.title_without_series, ar.image_url, ar.book_url,
               ar.num_pages, ar.format, ar.edition_information, ar.publisher, ar.average_rating, ar.ratings_count,
               ar.description, ar.authors, ar.record_create_timestamp
        FROM goodreads_analytics.popular_books_average_rating ar
        INNER JOIN
        goodreads_analytics.popular_books_review_count rc
        ON ar.book_id = rc.book_id;
    """