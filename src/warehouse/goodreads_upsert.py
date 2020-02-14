# ==============================AUTHORS==========================================

upsert_authors = """
BEGIN TRANSACTION;

DELETE FROM goodreads_warehouse.authors
using goodreads_staging.authors
where goodreads_warehouse.authors.author_id = goodreads_staging.authors.author_id;

INSERT INTO goodreads_warehouse.authors
SELECT * FROM goodreads_staging.authors;

END TRANSACTION ;
COMMIT;
"""

# =============================REVIEWS==============================================

upsert_reviews = """
BEGIN TRANSACTION;

DELETE FROM goodreads_warehouse.reviews
using goodreads_staging.reviews
where goodreads_warehouse.reviews.review_id = goodreads_staging.reviews.review_id;

INSERT INTO goodreads_warehouse.reviews
SELECT * FROM goodreads_staging.reviews;

END TRANSACTION ;
COMMIT;
"""

# ===============================BOOKS=============================================

upsert_books = """
BEGIN TRANSACTION;

DELETE FROM goodreads_warehouse.books
using goodreads_staging.books
where goodreads_warehouse.books.book_id = goodreads_staging.books.book_id;

INSERT INTO goodreads_warehouse.books
SELECT * FROM goodreads_staging.books;

END TRANSACTION ;
COMMIT;
"""

# ===============================USERS=============================================

upsert_users = """
BEGIN TRANSACTION;

DELETE FROM goodreads_warehouse.users
using goodreads_staging.users
where goodreads_warehouse.users.user_id = goodreads_staging.users.user_id;

INSERT INTO goodreads_warehouse.users
SELECT * FROM goodreads_staging.users;

END TRANSACTION ;
COMMIT;
"""

# ======================================================================================