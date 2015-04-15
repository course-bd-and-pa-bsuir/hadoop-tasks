//
// Task 3: e-library database
//

DROP KEYSPACE IF EXISTS library;
CREATE KEYSPACE IF NOT EXISTS library WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE library;

// Schema & data
DROP TABLE IF EXISTS books;
CREATE TABLE IF NOT EXISTS books (
    id VARCHAR,
    title VARCHAR,
    authors MAP<VARCHAR, VARCHAR>,
    published TIMESTAMP,
    PRIMARY KEY (id)
);

INSERT INTO books (id, title, authors, published) VALUES ('book1', 'Hunters Of Glor?', {'author1': 'Steve Jobs'}, '2000-03-24');
INSERT INTO books (id, title, authors, published) VALUES ('book2', 'Assassins And Wolves', {'author1': 'Steve Jobs', 'author2': 'Mark Zuckerberg'}, '2000-05-31');
INSERT INTO books (id, title, authors, published) VALUES ('book3', 'Descendants Of The Galaxy', {'author3': 'Robert Zemeckis', 'author4': 'Ashton Kutcher', 'author5': 'George R. R. Martin'}, '1995-09-25');
INSERT INTO books (id, title, authors, published) VALUES ('book4', 'Painting The Catacombs', {'author4': 'Ashton Kutcher'}, '2008-11-22');
INSERT INTO books (id, title, authors, published) VALUES ('book5', 'Criminal And Woman', {'author1': 'Steve Jobs', 'author5': 'George R. R. Martin'}, '1999-12-11');

DROP TABLE IF EXISTS authors;
CREATE TABLE IF NOT EXISTS authors (
    id VARCHAR,
    name VARCHAR,
    total_books INT, // "either all the columns of a table outside the PRIMARY KEY have the counter type, or none of them have it." :((( Seems we should create extra table(-s) for counters
    PRIMARY KEY (id)
);

INSERT INTO authors (id, name, total_books) VALUES ('author1', 'Steve Job?', 3);
INSERT INTO authors (id, name, total_books) VALUES ('author2', 'Mark Zuckerberg', 1);
INSERT INTO authors (id, name, total_books) VALUES ('author3', 'Robert Zemeckis', 1);
INSERT INTO authors (id, name, total_books) VALUES ('author4', 'Ashton Kutcher', 2);
INSERT INTO authors (id, name, total_books) VALUES ('author5', 'George R. R. Martin', 1);

DROP TABLE IF EXISTS books_by_authors;
CREATE TABLE IF NOT EXISTS books_by_authors (
    author_id VARCHAR,
    book_id VARCHAR,
    title VARCHAR,
    published TIMESTAMP,
    PRIMARY KEY (author_id, published)
);

INSERT INTO books_by_authors (author_id, book_id, title, published) VALUES ('author1', 'book1', 'Hunters Of Glory', '2000-03-24');
INSERT INTO books_by_authors (author_id, book_id, title, published) VALUES ('author1', 'book2', 'Assassins And Wolves', '2000-05-31');
INSERT INTO books_by_authors (author_id, book_id, title, published) VALUES ('author1', 'book5', 'Criminal And Woman', '1999-12-11');
INSERT INTO books_by_authors (author_id, book_id, title, published) VALUES ('author2', 'book2', 'Assassins And Wolves', '2000-05-31');
INSERT INTO books_by_authors (author_id, book_id, title, published) VALUES ('author3', 'book3', 'Descendants Of The Galaxy', '1995-09-25');
INSERT INTO books_by_authors (author_id, book_id, title, published) VALUES ('author4', 'book3', 'Descendants Of The Galaxy', '1995-09-25');
INSERT INTO books_by_authors (author_id, book_id, title, published) VALUES ('author4', 'book4', 'Painting The Catacombs', '2008-11-22');
INSERT INTO books_by_authors (author_id, book_id, title, published) VALUES ('author5', 'book1', 'Hunters Of Glory', '2000-03-24');
INSERT INTO books_by_authors (author_id, book_id, title, published) VALUES ('author5', 'book5', 'Criminal And Woman', '1999-12-11');

DROP TABLE IF EXISTS books_by_years;
CREATE TABLE IF NOT EXISTS books_by_years (
    year INT,
    book_id VARCHAR,
    title VARCHAR,
    published TIMESTAMP,
    PRIMARY KEY (year, published)
);

INSERT INTO books_by_years (year, book_id, title, published) VALUES (2000, 'book1', 'Hunters Of Glory', '2000-03-24');
INSERT INTO books_by_years (year, book_id, title, published) VALUES (2000, 'book2', 'Assassins And Wolves', '2000-05-31');
INSERT INTO books_by_years (year, book_id, title, published) VALUES (1995, 'book3', 'Descendants Of The Galaxy', '1995-09-25');
INSERT INTO books_by_years (year, book_id, title, published) VALUES (2008, 'book4', 'Painting The Catacombs', '2008-11-22');
INSERT INTO books_by_years (year, book_id, title, published) VALUES (1999, 'book5', 'Criminal And Woman', '1999-12-11');

// Extra index

CREATE INDEX ON authors (name);

// Update queries

UPDATE authors SET name = 'Steve Jobs' WHERE id = 'author1';
UPDATE books SET title = 'Hunters Of Glory' WHERE id = 'book1';

// Queries

// Find a book by ID
SELECT * FROM books WHERE id = 'book5';

// Results:
 id    | authors                                                     | published                | title
-------+-------------------------------------------------------------+--------------------------+--------------------
 book5 | {'author1': 'Steve Jobs', 'author5': 'George R. R. Martin'} | 1999-12-11 00:00:00+0300 | Criminal And Woman

(1 rows)

// Find all books by author. Ordered by publication date (ascending)
SELECT * FROM books_by_authors WHERE author_id = 'author1';

// Results:
 author_id | published                | book_id | title
-----------+--------------------------+---------+----------------------
   author1 | 1999-12-11 00:00:00+0300 |   book5 |   Criminal And Woman
   author1 | 2000-03-24 00:00:00+0300 |   book1 |     Hunters Of Glory
   author1 | 2000-05-31 00:00:00+0300 |   book2 | Assassins And Wolves

(3 rows)

// Find an author by ID
SELECT * FROM authors WHERE id = 'author1';
SELECT * FROM authors WHERE name = 'Steve Jobs';

// Results:
 id      | name       | total_books
---------+------------+-------------
 author1 | Steve Jobs |           3

(1 rows)

// Find books by year. Ordered by publication date (ascending)
SELECT * FROM books_by_years WHERE year = 2000;

// Results:
 year | published                | book_id | title
------+--------------------------+---------+----------------------
 2000 | 2000-03-24 00:00:00+0300 |   book1 |     Hunters Of Glory
 2000 | 2000-05-31 00:00:00+0300 |   book2 | Assassins And Wolves

(2 rows)
