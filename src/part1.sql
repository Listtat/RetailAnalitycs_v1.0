SET DATESTYLE to iso, DMY;

--  Создание таблицы personal_information
DROP TABLE IF EXISTS personal_information CASCADE;

CREATE TABLE personal_information (
    Customer_ID            bigint PRIMARY KEY NOT NULL,
    Customer_Name          varchar NOT NULL,
    Customer_Surname       varchar NOT NULL,
    Customer_Primary_Email varchar NOT NULL UNIQUE,
    Customer_Primary_Phone varchar NOT NULL UNIQUE,
    CHECK (Customer_Name ~ '^([А-Я]{1}[а-яё\- ]{0,}|[A-Z]{1}[a-z\- ]{0,})$'),
    CHECK (Customer_Surname ~  '^([А-Я]{1}[а-яё\- ]{0,}|[A-Z]{1}[a-z\- ]{0,})$'),
    CHECK (Customer_Primary_Email ~ '^((([0-9A-Za-z]{1}[-0-9A-z\.]{0,}[0-9A-Za-z]{1})|([0-9А-Яа-я]{1}[-0-9А-я\.]{0,}[0-9А-Яа-я]{1}))@([-A-Za-z]{1,}\.){1,2}[-A-Za-z]{2,})$'),
    CHECK (Customer_Primary_Phone ~ '^((\+7)+([0-9]){10})$'));

-- Триггерная функция, которая перед вставкой в таблицу personal_information проверяет, чтобы
-- имя и фамилия были написаны на одной раскладке
CREATE OR REPLACE FUNCTION check_insert_name_surname() RETURNS TRIGGER AS $trg_check_insert_name_surname$
    BEGIN
         IF ((NEW.Customer_Name ~ '[А-Я]{1}[а-яё]{0,}') AND (NEW.Customer_Surname ~ '[A-Z]{1}[a-z]{0,}') OR
             (NEW.Customer_Name ~ '[A-Z]{1}[a-z]{0,}') AND (NEW.Customer_Surname ~ '[А-Я]{1}[а-яё]{0,}')) THEN
             RAISE EXCEPTION 'Имя и фамилия должны быть в одной раскладке';
         END IF;
         RETURN NEW;
    END;
$trg_check_insert_name_surname$ LANGUAGE plpgsql;

-- Триггер для функции check_insert_name_surname
CREATE TRIGGER trg_check_insert_name_surname
BEFORE INSERT ON personal_information
    FOR EACH ROW EXECUTE FUNCTION check_insert_name_surname();


--  Создание таблицы Cards
DROP TABLE IF EXISTS Cards CASCADE;

CREATE TABLE Cards (
    Customer_Card_ID bigint PRIMARY KEY NOT NULL,
    Customer_ID bigint NOT NULL,
    FOREIGN KEY (Customer_ID) REFERENCES personal_information(Customer_ID));


--  Создание таблицы SKU_group
DROP TABLE IF EXISTS SKU_group CASCADE;

CREATE TABLE SKU_group (
    Group_ID bigint PRIMARY KEY NOT NULL,
    Group_Name varchar NOT NULL,
    CHECK (Group_Name ~ '^[а-яА-ЯёЁ0-9!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?]*|[a-zA-Z0-9!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?]*$'));


--  Создание таблицы Product_grid
DROP TABLE IF EXISTS Product_grid CASCADE;

CREATE TABLE Product_grid (
    SKU_ID bigint PRIMARY KEY NOT NULL,
    SKU_Name varchar NOT NULL,
    Group_ID bigint NOT NULL,
    FOREIGN KEY (Group_ID) REFERENCES  SKU_group(Group_ID),
    CHECK (SKU_Name ~ '^[а-яА-ЯёЁ0-9!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?]*|[a-zA-Z0-9!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?]*$'));


--  Создание таблицы Stores
DROP TABLE IF EXISTS Stores CASCADE;

CREATE TABLE Stores (
    Transaction_Store_ID bigint NOT NULL,
    SKU_ID bigint NOT NULL,
    SKU_Purchase_Price double precision NOT NULL,
    SKU_Retail_Price double precision NOT NULL,
    FOREIGN KEY (SKU_ID) REFERENCES Product_grid(SKU_ID));


--  Создание таблицы Transactions
DROP TABLE IF EXISTS Transactions CASCADE;

CREATE TABLE Transactions (
    Transaction_ID bigint PRIMARY KEY NOT NULL,
    Customer_Card_ID bigint NOT NULL,
    Transaction_Summ double precision NOT NULL,
    Transaction_DateTime timestamp NOT NULL,
    Transaction_Store_ID bigint NOT NULL,
    FOREIGN KEY (Customer_Card_ID) REFERENCES Cards(Customer_Card_ID));


--  Создание таблицы Checks
DROP TABLE IF EXISTS Checks CASCADE;

CREATE TABLE Checks (
    Transaction_ID bigint PRIMARY KEY NOT NULL,
    SKU_ID bigint NOT NULL,
    SKU_Amount double precision NOT NULL,
    SKU_Summ double precision NOT NULL,
    SKU_Summ_Paid double precision NOT NULL,
    SKU_Discount double precision NOT NULL,
    FOREIGN KEY (Transaction_ID) REFERENCES Transactions(Transaction_ID),
    FOREIGN KEY (SKU_ID) REFERENCES Product_grid(SKU_ID)
);


--  Создание таблицы Date_of_analysis_formation
DROP TABLE IF EXISTS Date_of_analysis_formation CASCADE;

CREATE TABLE Date_of_analysis_formation (
    Analysis_Formation timestamp
);


-- Создание процедуры для импорта данных из файлов
DROP PROCEDURE IF EXISTS import() CASCADE;

CREATE OR REPLACE PROCEDURE import(IN tablename varchar, IN path text, IN separator char DEFAULT '\t') AS $$
    BEGIN
        IF (separator = '\t') THEN
            EXECUTE format('COPY %s FROM ''%s'' DELIMITER E''%s'' CSV HEADER;', tablename, path, separator);
        ELSE
            EXECUTE format('COPY %s FROM ''%s'' DELIMITER ''%s'' CSV HEADER;', tablename, path, separator);
        END IF;
    END;
$$ LANGUAGE plpgsql;


-- Создание процедуры для экпорта данных в файлы
DROP PROCEDURE IF EXISTS export() CASCADE;

CREATE OR REPLACE PROCEDURE export(IN tablename varchar, IN path text, IN separator char DEFAULT '\t') AS $$
    BEGIN
        IF (separator = '\t') THEN
            EXECUTE format('COPY %s TO ''%s'' DELIMITER E''%s'' CSV HEADER;', tablename, path, separator);
        ELSE
            EXECUTE format('COPY %s TO ''%s'' DELIMITER ''%s'' CSV HEADER;', tablename, path, separator);
        END IF;
    END;
$$ LANGUAGE plpgsql;


-- ***************CHECK PROCEDURES****************
-- ***************IMPORT FROM TSV*****************
-- CALL import('Personal_information', '/Users/changeli/goinfre/SQL_3/datasets/Personal_information.tsv');
-- CALL import('Cards', '/Users/changeli/goinfre/SQL_3/datasets/Cards.tsv');
-- CALL import('Transactions', '/Users/changeli/goinfre/SQL_3/datasets/Transactions.tsv');
-- CALL import('SKU_group', '/Users/changeli/goinfre/SQL_3/datasets/SKU_group.tsv');
-- CALL import('Product_grid', '/Users/changeli/goinfre/SQL_3/datasets/Product_grid.tsv');
-- CALL import('Checks', '/Users/changeli/goinfre/SQL_3/datasets/Checks.tsv');
-- CALL import('Date_of_analysis_formation', '/Users/changeli/goinfre/SQL_3/datasets/Date_of_analysis_formation.tsv');
-- CALL import('Stores', '/Users/changeli/goinfre/SQL_3/datasets/Stores.tsv');

-- ***************EXPORT FROM TSV*****************
-- CALL export('Personal_information', '/Users/changeli/goinfre/SQL_3/src/Personal_information.tsv');
-- CALL export('Cards', '/Users/changeli/goinfre/SQL_3/src/Cards.tsv');
-- CALL export('Transactions', '/Users/changeli/goinfre/SQL_3/src/Transactions.tsv');
-- CALL export('SKU_group', '/Users/changeli/goinfre/SQL_3/src/SKU_group.tsv');
-- CALL export('Product_grid', '/Users/changeli/goinfre/SQL_3/src/Product_grid.tsv');
-- CALL export('Checks', '/Users/changeli/goinfre/SQL_3/src/Checks.tsv');
-- CALL export('Date_of_analysis_formation', '/Users/changeli/goinfre/SQL_3/src/Date_of_analysis_formation.tsv');
-- CALL export('Stores', '/Users/changeli/goinfre/SQL_3/src/Stores.tsv');


-- ***************EXPORT FROM CSV*****************
-- CALL export('Personal_information', '/Users/changeli/goinfre/SQL_3/src/Personal_information.csv', ',');
-- CALL export('Cards', '/Users/changeli/goinfre/SQL_3/src/Cards.csv', ',');
-- CALL export('Transactions', '/Users/changeli/goinfre/SQL_3/src/Transactions.csv', ',');
-- CALL export('SKU_group', '/Users/changeli/goinfre/SQL_3/src/SKU_group.csv', ',');
-- CALL export('Product_grid', '/Users/changeli/goinfre/SQL_3/src/Product_grid.csv', ',');
-- CALL export('Checks', '/Users/changeli/goinfre/SQL_3/src/Checks.csv', ',');
-- CALL export('Date_of_analysis_formation', '/Users/changeli/goinfre/SQL_3/src/Date_of_analysis_formation.csv', ',');
-- CALL export('Stores', '/Users/changeli/goinfre/SQL_3/src/Stores.csv', ',');


-- ***************IMPORT FROM CSV*****************
-- CALL import('Personal_information', '/Users/changeli/goinfre/SQL_3/src/Personal_information.csv', ',');
-- CALL import('Cards', '/Users/changeli/goinfre/SQL_3/src/Cards.csv', ',');
-- CALL import('Transactions', '/Users/changeli/goinfre/SQL_3/src/Transactions.csv', ',');
-- CALL import('SKU_group', '/Users/changeli/goinfre/SQL_3/src/SKU_group.csv', ',');
-- CALL import('Product_grid', '/Users/changeli/goinfre/SQL_3/src/Product_grid.csv', ',');
-- CALL import('Checks', '/Users/changeli/goinfre/SQL_3/src/Checks.csv', ',');
-- CALL import('Date_of_analysis_formation', '/Users/changeli/goinfre/SQL_3/src/Date_of_analysis_formation.csv', ',');
-- CALL import('Stores', '/Users/changeli/goinfre/SQL_3/src/Stores.csv', ',');


-- TRUNCATE TABLE Cards CASCADE;
-- TRUNCATE TABLE Checks CASCADE;
-- TRUNCATE TABLE Date_of_analysis_formation CASCADE;
-- TRUNCATE TABLE personal_information CASCADE;
-- TRUNCATE TABLE product_grid CASCADE;
-- TRUNCATE TABLE SKU_group CASCADE;
-- TRUNCATE TABLE Stores CASCADE;
-- TRUNCATE TABLE Transactions CASCADE;


-- DROP TABLE Cards CASCADE;
-- DROP TABLE Checks CASCADE;
-- DROP TABLE Date_of_analysis_formation CASCADE;
-- DROP TABLE personal_information CASCADE;
-- DROP TABLE product_grid CASCADE;
-- DROP TABLE SKU_group CASCADE;
-- DROP TABLE Stores CASCADE;
-- DROP TABLE Transactions CASCADE;

