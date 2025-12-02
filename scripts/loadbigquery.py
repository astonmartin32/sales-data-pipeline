import os
import pandas as pd
from google.cloud import bigquery

# Detectar carpetas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")

# Ruta al credentials.json (debe estar en la raíz del proyecto)
CREDS_PATH = os.path.join(BASE_DIR, "credentials.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDS_PATH

def load_to_bigquery():
    # Cliente de BigQuery
    client = bigquery.Client()

    # CAMBIA ESTO por tu project id real si es distinto
    project_id = "data-pipeline-project-479414"
    dataset_id = "sales"

    table_tx = f"{project_id}.{dataset_id}.transactions"
    table_monthly = f"{project_id}.{dataset_id}.monthly_revenue"

    # Leer CSV de transacciones limpias
    tx_path = os.path.join(CLEAN_DIR, "sales_transactions_clean.csv")
    print("Leyendo transacciones desde:", tx_path)
    df_tx = pd.read_csv(tx_path)

    # Leer CSV de agregación mensual
    monthly_path = os.path.join(CLEAN_DIR, "sales_monthly_agg.csv")
    print("Leyendo agregación mensual desde:", monthly_path)
    df_monthly = pd.read_csv(monthly_path)

    # Configuración de carga (sobrescribe y autodetecta esquema)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    # Cargar tabla de transacciones
    print(f"Cargando transacciones en {table_tx}...")
    job_tx = client.load_table_from_dataframe(df_tx, table_tx, job_config=job_config)
    job_tx.result()

    # Cargar tabla mensual
    print(f"Cargando agregación mensual en {table_monthly}...")
    job_monthly = client.load_table_from_dataframe(df_monthly, table_monthly, job_config=job_config)
    job_monthly.result()

    table1 = client.get_table(table_tx)
    table2 = client.get_table(table_monthly)

    print(f"Transacciones: {table1.num_rows} filas, {len(table1.schema)} columnas")
    print(f"Mensual: {table2.num_rows} filas, {len(table2.schema)} columnas")
    print("Carga a BigQuery completada.")

if __name__ == "__main__":
    load_to_bigquery()
