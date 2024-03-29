CREATE DATABASE library;

USE library;

-- Tabela Livro
CREATE TABLE book(
    id_book INT PRIMARY KEY AUTO_INCREMENT,
    work_key VARCHAR(20),
    isbn_id VARCHAR(1000),
    title VARCHAR(500),
    author_keys VARCHAR(1000),
    genre VARCHAR(1000),
    first_publish_year INT,
    number_of_pages_median INT,
    edition_count INT,
    edition_keys VARCHAR(1000)
);

-- Tabela Autor
CREATE TABLE author(
    author_key VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    full_name VARCHAR(255),
    alternate_names VARCHAR(255),
    birth_date VARCHAR(255)
);

-- Tabela Cliente
CREATE TABLE customer(
    id_customer INT PRIMARY KEY AUTO_INCREMENT,
    full_name VARCHAR(255),
    birth_date DATE,
    genre VARCHAR(6),
    document VARCHAR(11) NOT NULL UNIQUE,
    cellphone_number VARCHAR(20),
    birth_state VARCHAR (30),
    birth_city VARCHAR(100),
    current_postal_code VARCHAR(9),
    registration_date DATE
);

-- Tabela Aluguel (Evento)
CREATE TABLE rent(
    id_rent INT NOT NULL AUTO_INCREMENT,
    id_book INT,
    id_customer INT,
    event_date DATE,
    expected_return_date DATE,
    event_type VARCHAR(50),
    rental_value DECIMAL(10, 2),
    penalty_value DECIMAL(10, 2),
    FOREIGN KEY (id_book) REFERENCES book(id_book),
    FOREIGN KEY (id_customer) REFERENCES customer(id_customer),
    PRIMARY KEY (id_book, id_customer, event_date, event_type),
    INDEX id (id_rent)
);


DROP TABLE rent;
DROP TABLE author;
DROP TABLE book;
DROP TABLE customer;