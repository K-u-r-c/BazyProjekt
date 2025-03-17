import csv
import psycopg2
import mysql.connector
import sqlite3
from pymongo import MongoClient
import json
from datetime import datetime

# Database connection parameters (replace with your actual credentials)
POSTGRES_PARAMS = {
    "dbname": "mydb",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5432",
}

MYSQL_PARAMS = {
    "user": "root",
    "password": "password",
    "host": "localhost",
    "database": "mydb",
    "port": "3306",
}

SQLITE_DB = "mydb.db"
MONGODB_URI = "mongodb://localhost:27017/"
MONGODB_DB = "mydb"

def create_postgres_tables(conn):
    """Creates tables in the PostgreSQL database."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            client_id INT PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(20),
            address VARCHAR(255),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS categories (
            category_id INT PRIMARY KEY,
            category_name VARCHAR(255),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY,
            product_ean VARCHAR(20),
            product_name VARCHAR(255),
            category_id INT,
            stock INT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS product_prices (
            product_price_id INT PRIMARY KEY,
            product_id INT,
            price DECIMAL(10, 2),
            currency VARCHAR(3),
            effective_date TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS product_descriptions (
            product_desc_id INT PRIMARY KEY,
            product_id INT,
            description TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT PRIMARY KEY,
            client_id INT,
            order_date TIMESTAMP,
            total_amount DECIMAL(10, 2),
            order_status VARCHAR(20),
            items JSON,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()

def create_mysql_tables(conn):
    """Creates tables in the MySQL database."""
    cur = conn.cursor()
    queries = [
        """
        CREATE TABLE IF NOT EXISTS clients (
            client_id INT PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(20),
            address VARCHAR(255),
            created_at DATETIME,
            updated_at DATETIME
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS categories (
            category_id INT PRIMARY KEY,
            category_name VARCHAR(255),
            created_at DATETIME,
            updated_at DATETIME
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY,
            product_ean VARCHAR(20),
            product_name VARCHAR(255),
            category_id INT,
            stock INT,
            created_at DATETIME,
            updated_at DATETIME
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS product_prices (
            product_price_id INT PRIMARY KEY,
            product_id INT,
            price DECIMAL(10, 2),
            currency VARCHAR(3),
            effective_date DATETIME,
            created_at DATETIME,
            updated_at DATETIME
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS product_descriptions (
            product_desc_id INT PRIMARY KEY,
            product_id INT,
            description TEXT,
            created_at DATETIME,
            updated_at DATETIME
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT PRIMARY KEY,
            client_id INT,
            order_date DATETIME,
            total_amount DECIMAL(10, 2),
            order_status VARCHAR(20),
            items JSON,
            created_at DATETIME,
            updated_at DATETIME
        );
        """,
    ]
    for query in queries:
        cur.execute(query)
        conn.commit()
    cur.close()

def create_sqlite_tables(conn):
    """Creates tables in the SQLite database."""
    cur = conn.cursor()
    queries = [
        """
        CREATE TABLE IF NOT EXISTS clients (
            client_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS categories (
            category_id INTEGER PRIMARY KEY,
            category_name TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            product_ean TEXT,
            product_name TEXT,
            category_id INTEGER,
            stock INTEGER,
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS product_prices (
            product_price_id INTEGER PRIMARY KEY,
            product_id INTEGER,
            price REAL,
            currency TEXT,
            effective_date TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS product_descriptions (
            product_desc_id INTEGER PRIMARY KEY,
            product_id INTEGER,
            description TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            client_id INTEGER,
            order_date TEXT,
            total_amount REAL,
            order_status TEXT,
            items TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """,
    ]
    for query in queries:
        cur.execute(query)
        conn.commit()
    cur.close()

def upload_csv_to_postgres(csv_file, table_name, columns):
    """Uploads a CSV file to a PostgreSQL database using COPY FROM."""
    try:
        conn = psycopg2.connect(**POSTGRES_PARAMS)
        cur = conn.cursor()

        with open(csv_file, 'r', encoding='utf-8') as f:
            next(f)  # Skip header row
            cur.copy_from(f, table_name, sep=',', columns=tuple(columns))

        conn.commit()
        print(f"Uploaded {csv_file} to PostgreSQL table {table_name} (COPY FROM)")
    except psycopg2.Error as e:
        print(f"Error uploading to PostgreSQL: {e}")
        conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def upload_csv_to_mysql(csv_file, table_name, columns):
    """Uploads a CSV file to a MySQL database using LOAD DATA INFILE."""
    try:
        conn = mysql.connector.connect(**MYSQL_PARAMS)
        cur = conn.cursor()
        cur.execute("SET GLOBAL local_infile = 1;")
        cur.execute("SET SESSION local_infile = 1;")

        query = f"""
            LOAD DATA LOCAL INFILE '{csv_file.replace("\\", "/")}'
            INTO TABLE {table_name}
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS;
        """
        cur.execute(query)
        conn.commit()
        print(f"Uploaded {csv_file} to MySQL table {table_name} (LOAD DATA INFILE)")
    except mysql.connector.Error as e:
        print(f"Error uploading to MySQL: {e}")
        conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def upload_csv_to_sqlite(csv_file, table_name, columns):
    """Uploads a CSV file to a SQLite database using executemany."""
    try:
        conn = sqlite3.connect(SQLITE_DB)
        cur = conn.cursor()

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            placeholders = ', '.join(['?'] * len(columns))
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            cur.executemany(query, reader)

        conn.commit()
        print(f"Uploaded {csv_file} to SQLite table {table_name} (executemany)")
    except sqlite3.Error as e:
        print(f"Error uploading to SQLite: {e}")
        conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def upload_csv_to_mongodb(csv_file, collection_name, columns):
    """Uploads a CSV file to a MongoDB collection."""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DB]
        collection = db[collection_name]

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)
            collection.insert_many(data)

        print(f"Uploaded {csv_file} to MongoDB collection {collection_name}")
    except Exception as e:
        print(f"Error uploading to MongoDB: {e}")
    finally:
        if 'client' in locals() and client:
            client.close()

if __name__ == "__main__":
    # Create tables before uploading data
    try:
        postgres_conn = psycopg2.connect(**POSTGRES_PARAMS)
        create_postgres_tables(postgres_conn)
        postgres_conn.close()
    except psycopg2.Error as e:
        print(f"Error creating PostgreSQL tables: {e}")

    try:
        mysql_conn = mysql.connector.connect(**MYSQL_PARAMS)
        create_mysql_tables(mysql_conn)
        mysql_conn.close()
    except mysql.connector.Error as e:
        print(f"Error creating MySQL tables: {e}")

    try:
        sqlite_conn = sqlite3.connect(SQLITE_DB)
        create_sqlite_tables(sqlite_conn)
        sqlite_conn.close()
    except sqlite3.Error as e:
        print(f"Error creating SQLite tables: {e}")
        
    # Example usage:
    upload_csv_to_postgres("clients.csv", "clients", ["client_id", "first_name", "last_name", "email", "phone", "address", "created_at", "updated_at"])
    upload_csv_to_mysql("clients.csv", "clients", ["client_id", "first_name", "last_name", "email", "phone", "address", "created_at", "updated_at"])
    upload_csv_to_sqlite("clients.csv", "clients", ["client_id", "first_name", "last_name", "email", "phone", "address", "created_at", "updated_at"])
    upload_csv_to_mongodb("clients.csv", "clients", ["client_id", "first_name", "last_name", "email", "phone", "address", "created_at", "updated_at"])

    upload_csv_to_postgres("categories.csv", "categories", ["category_id", "category_name", "created_at", "updated_at"])
    upload_csv_to_mysql("categories.csv", "categories", ["category_id", "category_name", "created_at", "updated_at"])
    upload_csv_to_sqlite("categories.csv", "categories", ["category_id", "category_name", "created_at", "updated_at"])
    upload_csv_to_mongodb("categories.csv", "categories", ["category_id", "category_name", "created_at", "updated_at"])

    upload_csv_to_postgres("products.csv", "products", ["product_id", "product_ean", "product_name", "category_id", "stock", "created_at", "updated_at"])
    upload_csv_to_mysql("products.csv", "products", ["product_id", "product_ean", "product_name", "category_id", "stock", "created_at", "updated_at"])
    upload_csv_to_sqlite("products.csv", "products", ["product_id", "product_ean", "product_name", "category_id", "stock", "created_at", "updated_at"])
    upload_csv_to_mongodb("products.csv", "products", ["product_id", "product_ean", "product_name", "category_id", "stock", "created_at", "updated_at"])

    upload_csv_to_postgres("product_prices.csv", "product_prices", ["product_price_id", "product_id", "price", "currency", "effective_date", "created_at", "updated_at"])
    upload_csv_to_mysql("product_prices.csv", "product_prices", ["product_price_id", "product_id", "price", "currency", "effective_date", "created_at", "updated_at"])
    upload_csv_to_sqlite("product_prices.csv", "product_prices", ["product_price_id", "product_id", "price", "currency", "effective_date", "created_at", "updated_at"])
    upload_csv_to_mongodb("product_prices.csv", "product_prices", ["product_price_id", "product_id", "price", "currency", "effective_date", "created_at", "updated_at"])

    upload_csv_to_postgres("product_descriptions.csv", "product_descriptions", ["product_desc_id", "product_id", "description", "created_at", "updated_at"])
    upload_csv_to_mysql("product_descriptions.csv", "product_descriptions", ["product_desc_id", "product_id", "description", "created_at", "updated_at"])
    upload_csv_to_sqlite("product_descriptions.csv", "product_descriptions", ["product_desc_id", "product_id", "description", "created_at", "updated_at"])
    upload_csv_to_mongodb("product_descriptions.csv", "product_descriptions", ["product_desc_id", "product_id", "description", "created_at", "updated_at"])