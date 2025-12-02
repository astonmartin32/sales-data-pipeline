import os
import pandas as pd
import numpy as np
from datetime import datetime

# Detectar carpeta raíz del proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

os.makedirs(RAW_DIR, exist_ok=True)

def generate_sales_data(n_rows: int = 3000) -> pd.DataFrame:
    """
    Genera un dataset sintético de ventas históricas.
    """
    np.random.seed(42)

    # Rango de fechas (2021-01-01 a 2024-12-31)
    dates = pd.date_range(start="2021-01-01", end="2024-12-31", freq="D")

    products = [
        ("iPhone 14", "Smartphones"),
        ("Samsung S23", "Smartphones"),
        ("MacBook Pro", "Laptops"),
        ("Dell XPS 13", "Laptops"),
        ("AirPods Pro", "Accessories"),
        ("Logitech MX Master 3", "Accessories"),
        ("Sony WH-1000XM5", "Headphones"),
        ("JBL Flip 6", "Speakers"),
        ("Apple Watch", "Wearables"),
        ("Fitbit Charge 6", "Wearables"),
    ]

    customer_ids = [f"CUST-{i:04d}" for i in range(1, 301)]

    rows = []
    for order_id in range(1, n_rows + 1):
        order_date = np.random.choice(dates)

        product_name, category = products[np.random.randint(0, len(products))]
        customer_id = np.random.choice(customer_ids)

        base_price = {
            "Smartphones": 900,
            "Laptops": 1500,
            "Accessories": 150,
            "Headphones": 300,
            "Speakers": 200,
            "Wearables": 250,
        }[category] + np.random.randint(-100, 101)

        quantity = np.random.randint(1, 5)

        rows.append(
            {
                "order_id": order_id,
                "order_date": order_date,
                "product": product_name,
                "category": category,
                "customer_id": customer_id,
                "price": float(base_price),
                "quantity": int(quantity),
            }
        )

    df = pd.DataFrame(rows)
    return df

def main():
    df = generate_sales_data(n_rows=3000)
    output_path = os.path.join(RAW_DIR, "sales_raw.csv")
    df.to_csv(output_path, index=False)
    print(f"✔ Dataset generado en: {output_path}")
    print(df.head())

if __name__ == "__main__":
    main()
