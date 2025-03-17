import psycopg2
import mysql.connector
import sqlite3
from pymongo import MongoClient
import json
from datetime import datetime
from faker import Faker
import random

fake = Faker('pl_PL')

def generate_data(num_clients=10, num_categories=5, num_products=20, num_orders=15):
    data = {
        "clients": [],
        "categories": [],
        "products": [],
        "product_prices": [],
        "product_descriptions": [],
        "orders": []
    }

    # Generowanie klientów
    for i in range(1, num_clients + 1):
        data["clients"].append({
            "client_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.address(),
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

    # Generowanie kategorii
    for i in range(1, num_categories + 1):
        data["categories"].append({
            "category_id": i,
            "category_name": fake.word().capitalize() + "y",
            "description": fake.sentence(),
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

    # Generowanie produktów
    for i in range(1, num_products + 1):
        category_id = random.randint(1, num_categories)
        data["products"].append({
            "product_id": i,
            "product_ean": fake.ean(),
            "product_name": fake.word().capitalize() + " Product",
            "category_id": category_id,
            "stock": random.randint(1, 100),
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

        # Generowanie cen produktów
        data["product_prices"].append({
            "product_price_id": i,
            "product_id": i,
            "price": round(random.uniform(10, 1000), 2),
            "currency": "PLN",
            "effective_date": datetime.now(),
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

        # Generowanie opisów produktów
        data["product_descriptions"].append({
            "product_desc_id": i,
            "product_id": i,
            "description": fake.text(),
            "language": "pl",
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

    # Generowanie zamówień
    for i in range(1, num_orders + 1):
        client_id = random.randint(1, num_clients)
        items = []
        total_amount = 0
        for j in range(random.randint(1, 3)):
            product_id = random.randint(1, num_products)
            quantity = random.randint(1, 5)
            unit_price = data["product_prices"][product_id - 1]["price"]
            total_price = unit_price * quantity
            items.append({
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": total_price
            })
            total_amount += total_price

        data["orders"].append({
            "order_id": i,
            "client_id": client_id,
            "order_date": datetime.now(),
            "total_amount": round(total_amount, 2),
            "order_status": random.choice(["pending", "completed", "cancelled"]),
            "items": json.dumps(items),
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

    return data

def insert_data_postgres(data):
    conn = psycopg2.connect(dbname="mydb", user="user", password="password", host="localhost", port="5432")
    cur = conn.cursor()
    insert_data(cur, data)
    conn.commit()
    conn.close()

def insert_data_postgres_pgvector(data):
    conn = psycopg2.connect(dbname="mydb", user="user", password="password", host="localhost", port="5433")
    cur = conn.cursor()
    insert_data(cur, data)
    conn.commit()
    conn.close()

def insert_data_mysql(data):
    conn = mysql.connector.connect(user="root", password="password", host="localhost", database="mydb", port="3306")
    cur = conn.cursor()
    insert_data(cur, data)
    conn.commit()
    conn.close()

def insert_data_sqlite(data):
    conn = sqlite3.connect("mydb.db")
    cur = conn.cursor()
    insert_data(cur, data)
    conn.commit()
    conn.close()

def insert_data_mongodb(data):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["mydb"]
    for collection, docs in data.items():
        if docs:
            db[collection].insert_many(docs)
    client.close()

def insert_data(cur, data):
    for collection, docs in data.items():
        for doc in docs:
            columns = ", ".join(doc.keys())
            values = ", ".join(["%s"] * len(doc))
            sql = f"INSERT INTO {collection} ({columns}) VALUES ({values})"
            cur.execute(sql, list(doc.values()))

if __name__ == "__main__":
    data = generate_data()
    insert_data_postgres(data)
    insert_data_postgres_pgvector(data)
    insert_data_mysql(data)
    insert_data_sqlite(data)
    insert_data_mongodb(data)
    print("Dane zostały wstawione do wszystkich baz danych.")