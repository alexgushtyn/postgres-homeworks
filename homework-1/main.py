import psycopg2
import csv


conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='Fkalipt808')


with open("north_data/employees_data.csv", 'r', newline='') as file:
    reader = csv.reader(file)
    next(reader)
    with conn.cursor() as cur:
        for row in reader:
            cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", (row))
        cur.execute("SELECT * FROM employees")
        conn.commit()
        rows = cur.fetchall()
    conn.close()



with open('north_data/customers_data.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    with conn.cursor() as cur:
        for row in reader:
                cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", (row))
        cur.execute("SELECT * FROM customers")
        conn.commit()
        rows = cur.fetchall()
    conn.close()

with open('north_data/orders_data.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    with conn.cursor() as cur:
        for row in reader:
                cur.execute("INSERT INTO order VALUES (%s, %s, %s, %s, %s)", (row))
        cur.execute("SELECT * FROM orders")
        conn.commit()
        rows = cur.fetchall()
    conn.close()