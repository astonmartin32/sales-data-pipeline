import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")

os.makedirs(CLEAN_DIR, exist_ok=True)

def transform_sales_transactions():
    raw_path = os.path.join(RAW_DIR, "sales_raw.csv")
    df = pd.read_csv(raw_path)

    # Asegurar tipo fecha
    df["order_date"] = pd.to_datetime(df["order_date"])

    # Features de tiempo
    df["year"] = df["order_date"].dt.year
    df["month"] = df["order_date"].dt.month
    df["year_month"] = df["order_date"].dt.to_period("M").astype(str)

    # Revenue
    df["revenue"] = df["price"] * df["quantity"]

    # Ordenar
    df = df.sort_values("order_date")

    return df

def build_monthly_agg(df: pd.DataFrame) -> pd.DataFrame:
    # Revenue por año-mes
    monthly = (
        df.groupby("year_month", as_index=False)
        .agg(
            total_revenue=("revenue", "sum"),
            total_orders=("order_id", "nunique"),
            total_quantity=("quantity", "sum"),
        )
        .sort_values("year_month")
    )

    # Convertir year_month a fecha para ordenar mejor
    monthly["year_month_date"] = pd.to_datetime(monthly["year_month"] + "-01")

    monthly = monthly.sort_values("year_month_date")

    # Crecimiento Month-over-Month (MoM)
    monthly["revenue_mom_growth"] = (
        monthly["total_revenue"].pct_change() * 100
    ).round(2)

    # Año y mes para YoY
    monthly["year"] = monthly["year_month_date"].dt.year
    monthly["month"] = monthly["year_month_date"].dt.month

    # Para YoY, comparamos contra mismo mes del año anterior
    monthly["revenue_yoy_growth"] = None

    # Creamos un índice auxiliar
    monthly_indexed = monthly.set_index(["year", "month"])

    for (y, m), row in monthly_indexed.iterrows():
        prev_year_key = (y - 1, m)
        if prev_year_key in monthly_indexed.index:
            prev_revenue = monthly_indexed.loc[prev_year_key, "total_revenue"]
            if prev_revenue != 0:
                yoy = ((row["total_revenue"] - prev_revenue) / prev_revenue) * 100
                monthly_indexed.loc[(y, m), "revenue_yoy_growth"] = round(yoy, 2)

    monthly = monthly_indexed.reset_index()

    return monthly

def main():
    df_tx = transform_sales_transactions()
    tx_path = os.path.join(CLEAN_DIR, "sales_transactions_clean.csv")
    df_tx.to_csv(tx_path, index=False)
    print(f"✔ Transacciones limpias guardadas en: {tx_path}")

    df_monthly = build_monthly_agg(df_tx)
    monthly_path = os.path.join(CLEAN_DIR, "sales_monthly_agg.csv")
    df_monthly.to_csv(monthly_path, index=False)
    print(f"✔ Agregación mensual guardada en: {monthly_path}")

    print("\nPreview transacciones:")
    print(df_tx.head())
    print("\nPreview mensual:")
    print(df_monthly.head())

if __name__ == "__main__":
    main()
