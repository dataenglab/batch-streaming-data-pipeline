import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker("ru_RU")

conn = psycopg2.connect(
    host="postgres-src", dbname="mydb", user="postgres", password="postgres"
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id INT,
    customer_name VARCHAR(100),
    product VARCHAR(100),
    category VARCHAR(50),
    amount VARCHAR(20),      -- нарочно текстом, как "грязные" данные
    order_date VARCHAR(30),  -- нарочно текстом, разные форматы
    created_at TIMESTAMP DEFAULT now()
);
""")

categories = ["Электроника", "Одежда", "Книги", "Еда", "Игрушки"]
date_formats = ["%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"]

for i in range(500):
    order_id = random.randint(1, 400)  # специально с повторами (дубликаты)
    dt = fake.date_between(start_date="-90d", end_date="today")
    fmt = random.choice(date_formats)

    cur.execute("""
        INSERT INTO raw_orders (order_id, customer_name, product, category, amount, order_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        order_id,
        fake.name(),
        fake.word(),
        random.choice(categories),
        str(round(random.uniform(100, 10000), 2)) if random.random() > 0.05 else None,
        dt.strftime(fmt)
    ))

conn.commit()
cur.close()
conn.close()
print("Готово: 500 строк добавлено в raw_orders")