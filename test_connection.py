
import os
import psycopg
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

try:
    # Establish connection to PostgreSQL
    with psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    ) as conn:

        print("Connected to PostgreSQL successfully!")

        # Create a cursor to execute SQL
        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT customer_id, customer_name, city
                FROM customers
                ORDER BY customer_id;
            """)

            rows = cursor.fetchall()

            print(f"Total customers: {len(rows)}")

            for row in rows:
                print(row)

except psycopg.Error as error:
    print("Database error:", error)
