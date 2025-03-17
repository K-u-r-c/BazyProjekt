import csv
import random
import json
from datetime import datetime, timedelta

# Parametry – liczby rekordów (dla testów możesz zmniejszyć NUM_PRODUCTS i NUM_ORDERS)
NUM_CLIENTS = 1000
NUM_CATEGORIES = 1000
NUM_PRODUCTS = 5000000   # 5 milionów produktów
NUM_ORDERS = 10000       # przykładowa liczba zamówień

# Przykładowe listy danych
first_names = ["Anna", "Piotr", "Katarzyna", "Tomasz", "Maria", "Jan", "Agnieszka", "Andrzej", "Ewa", "Robert"]
last_names = ["Kowalski", "Nowak", "Wiśniewski", "Wójcik", "Kowalczyk", "Kamiński", "Lewandowski", "Zieliński", "Szymański", "Woźniak"]

product_keywords = ["Smartfon", "Komputer Stacjonarny", "Laptop", "Lodówka", "Piekarnik", "Tablet", "Telewizor", "Drukarka", "Monitor", "Głośnik"]
companies = ["Samsung", "Apple", "Xiomi", "DELL", "HP", "Lenovo", "Sony", "LG", "Acer", "Asus"]

order_statuses = ["pending", "completed", "cancelled"]

# Funkcja do generowania losowej daty między dwoma datami
def random_date(start, end):
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

# Ustalamy zakres dat – ostatnie 5 lat
now = datetime.now()
start_date = now - timedelta(days=365*5)
end_date = now

#############################
# 1. Generowanie klientów (Clients)
#############################
with open("clients.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["client_id", "first_name", "last_name", "email", "phone", "address", "created_at", "updated_at"])
    for i in range(1, NUM_CLIENTS + 1):
        first = random.choice(first_names)
        last = random.choice(last_names)
        email = f"{first.lower()}_{last.lower()}@mail.com"
        phone = f"+48{random.randint(100000000, 999999999)}"
        address = f"Ulica {random.randint(1,200)}, Miasto"
        created = random_date(start_date, end_date)
        updated = random_date(created, end_date)
        writer.writerow([i, first, last, email, phone, address, created.isoformat(), updated.isoformat()])
print("Klienci wygenerowani.")

#############################
# 2. Generowanie kategorii (Categories)
#############################
with open("categories.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["category_id", "category_name", "created_at", "updated_at"])
    for i in range(1, NUM_CATEGORIES + 1):
        category_name = f"Kategoria {i}"
        created = random_date(start_date, end_date)
        updated = random_date(created, end_date)
        writer.writerow([i, category_name, created.isoformat(), updated.isoformat()])
print("Kategorie wygenerowane.")

#############################
# 3. Generowanie produktów (Products)
#############################
with open("products.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["product_id", "product_ean", "product_name", "category_id", "stock", "created_at", "updated_at"])
    for i in range(1, NUM_PRODUCTS + 1):
        # Losowy EAN: 13-cyfrowy ciąg
        product_ean = ''.join(random.choices("0123456789", k=13))
        # Nazwa produktu – "sklejka" losowo wybranych komponentów
        product_name = f"{random.choice(companies)} {random.choice(product_keywords)} {random.randint(100, 999)}"
        category_id = random.randint(1, NUM_CATEGORIES)
        stock = random.randint(0, 1000)
        created = random_date(start_date, end_date)
        updated = random_date(created, end_date)
        writer.writerow([i, product_ean, product_name, category_id, stock, created.isoformat(), updated.isoformat()])
        if i % 100000 == 0:
            print(f"Generated {i} products")
print("Produkty wygenerowane.")

#############################
# 4. Generowanie cen produktów (Product_Prices)
#############################
with open("product_prices.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["product_price_id", "product_id", "price", "currency", "effective_date", "created_at", "updated_at"])
    for i in range(1, NUM_PRODUCTS + 1):
        price = round(random.uniform(10, 2000), 2)
        currency = "PLN"
        effective_date = random_date(start_date, end_date)
        created = random_date(start_date, effective_date)
        updated = random_date(created, end_date)
        writer.writerow([i, i, price, currency, effective_date.isoformat(), created.isoformat(), updated.isoformat()])
        if i % 100000 == 0:
            print(f"Generated {i} product prices")
print("Ceny produktów wygenerowane.")

#############################
# 5. Generowanie opisów produktów (Product_Descriptions)
#############################
with open("product_descriptions.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["product_desc_id", "product_id", "description", "created_at", "updated_at"])
    for i in range(1, NUM_PRODUCTS + 1):
        description = f"Opis produktu: {random.choice(product_keywords)} {random.choice(companies)} – wysokiej jakości produkt."
        created = random_date(start_date, end_date)
        updated = random_date(created, end_date)
        writer.writerow([i, i, description, created.isoformat(), updated.isoformat()])
        if i % 100000 == 0:
            print(f"Generated {i} product descriptions")
print("Opisy produktów wygenerowane.")

#############################
# 6. Generowanie zamówień (Orders) z wbudowanymi pozycjami (JSON)
#############################
with open("orders.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "client_id", "order_date", "total_amount", "order_status", "items", "created_at", "updated_at"])
    for i in range(1, NUM_ORDERS + 1):
        client_id = random.randint(1, NUM_CLIENTS)
        order_date = random_date(start_date, end_date)
        num_items = random.randint(1, 5)
        items = []
        total_amount = 0
        for _ in range(num_items):
            prod_id = random.randint(1, NUM_PRODUCTS)
            quantity = random.randint(1, 10)
            # Dla uproszczenia generujemy losową cenę jednostkową
            unit_price = round(random.uniform(10, 2000), 2)
            total_price = round(quantity * unit_price, 2)
            total_amount += total_price
            items.append({
                "product_id": prod_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": total_price
            })
        order_status = random.choice(order_statuses)
        created = random_date(start_date, order_date)
        updated = random_date(created, end_date)
        writer.writerow([
            i, 
            client_id, 
            order_date.isoformat(), 
            round(total_amount, 2), 
            order_status, 
            json.dumps(items), 
            created.isoformat(), 
            updated.isoformat()
        ])
        if i % 1000 == 0:
            print(f"Generated {i} orders")
print("Zamówienia wygenerowane.")
