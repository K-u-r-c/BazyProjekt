import psycopg2
import mysql.connector
import sqlite3
from pymongo import MongoClient
import json
from datetime import datetime

def create_and_insert_postgres():
    conn = psycopg2.connect(dbname="mydb", user="user", password="password", host="localhost", port="5432")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS test (id INT PRIMARY KEY, name VARCHAR(255))")
    cur.execute("INSERT INTO test (id, name) VALUES (1, 'PostgreSQL Test')")
    conn.commit()
    cur.execute("SELECT * FROM test")
    result = cur.fetchone()
    print("PostgreSQL:", result)
    cur.close()
    conn.close()

def create_and_insert_mysql():
    conn = mysql.connector.connect(user="root", password="password", host="localhost", database="mydb", port="3306")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS test (id INT PRIMARY KEY, name VARCHAR(255))")
    cur.execute("INSERT INTO test (id, name) VALUES (1, 'MySQL Test')")
    conn.commit()
    cur.execute("SELECT * FROM test")
    result = cur.fetchone()
    print("MySQL:", result)
    cur.close()
    conn.close()

def create_and_insert_sqlite():
    conn = sqlite3.connect("mydb.db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)")
    cur.execute("INSERT INTO test (id, name) VALUES (1, 'SQLite Test')")
    conn.commit()
    cur.execute("SELECT * FROM test")
    result = cur.fetchone()
    print("SQLite:", result)
    cur.close()
    conn.close()

def create_and_insert_mongodb():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["mydb"]
    collection = db["test"]
    collection.insert_one({"id": 1, "name": "MongoDB Test"})
    result = collection.find_one({"id": 1})
    print("MongoDB:", result)
    client.close()

if __name__ == "__main__":
    create_and_insert_postgres()
    create_and_insert_mysql()
    create_and_insert_sqlite()
    create_and_insert_mongodb()
    print("Testy zakończone.")