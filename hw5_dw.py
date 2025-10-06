# -*- coding: utf-8 -*-
"""Airflow DAG: Alpha Vantage → Snowflake ETL (Free API, TSLA only)"""

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import pandas as pd


# -----------------------------
# DAG CONFIGURATION
# -----------------------------
default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="alpha_vantage_to_snowflake_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ETL", "Snowflake", "AlphaVantage"],
    description="ETL pipeline: Alpha Vantage API → Snowflake (Free plan, TSLA only)",
) as dag:

    # -----------------------------
    # Step 1: EXTRACT TASK
    # -----------------------------
    @task
    def extract():
        """
        Extract daily stock data for TSLA from Alpha Vantage (free endpoint).
        """
        api_key = Variable.get("alpha_vantage_api_key")
        symbol = "TSLA"

        print(f"Fetching data for {symbol} from Alpha Vantage API...")

        url = (
            f"https://www.alphavantage.co/query?"
            f"function=TIME_SERIES_DAILY&symbol={symbol}"
            f"&apikey={api_key}&outputsize=compact"
        )

        response = requests.get(url)
        data = response.json()

        # Validate API response
        if "Time Series (Daily)" not in data:
            raise ValueError(f"Invalid API response for {symbol}: {data}")

        time_series_data = data["Time Series (Daily)"]

        # Convert JSON to DataFrame
        df = pd.DataFrame.from_dict(time_series_data, orient="index")
        df.reset_index(inplace=True)

        # Rename columns to clean names
        df.rename(
            columns={
                "index": "date",
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "close",
                "5. volume": "volume",
            },
            inplace=True,
        )

        # Verify all expected columns exist
        expected_cols = ["date", "open", "high", "low", "close", "volume"]
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns {missing} in API response")

        # Add symbol column
        df["symbol"] = symbol
        df = df[["symbol", "date", "open", "high", "low", "close", "volume"]]

        print(f"Extracted {len(df)} rows for {symbol}")
        return df.to_json(orient="records")

    # -----------------------------
    # Step 2: TRANSFORM TASK
    # -----------------------------
    @task
    def transform(json_data):
        """
        Clean and prepare data for Snowflake load.
        """
        df = pd.read_json(json_data)
        print("Transforming data...")

        df["date"] = pd.to_datetime(df["date"])
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        df = df.sort_values("date")

        print(f"Transformed {len(df)} {df['symbol'].iloc[0]} records")
        return df.to_json(orient="records")

    # -----------------------------
    # Step 3: LOAD TASK
    # -----------------------------
    @task
    def load_to_snowflake(json_data):
        """
        Full-refresh load into Snowflake using SQL transaction.
        """
        df = pd.read_json(json_data)
        hook = SnowflakeHook(snowflake_conn_id="my_snowflake_connection")
        conn = hook.get_conn()
        cur = conn.cursor()

        database = Variable.get("snowflake_database", default_var="RAW")
        schema = "RAW"
        target_table = f"{database}.{schema}.STOCK_PRICES"

        try:
            print("Loading data into Snowflake...")
            cur.execute("BEGIN;")

            # Ensure schema/table exists
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            cur.execute(f"""
                CREATE OR REPLACE TABLE {target_table} (
                    SYMBOL STRING,
                    DATE DATE,
                    OPEN FLOAT,
                    HIGH FLOAT,
                    LOW FLOAT,
                    CLOSE FLOAT,
                    VOLUME FLOAT
                );
            """)

            # Insert all rows (full refresh)
            for _, row in df.iterrows():
                cur.execute(
                    f"""
                    INSERT INTO {target_table}
                    (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        row["symbol"],
                        row["date"].strftime("%Y-%m-%d"),
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        float(row["volume"]),
                    ),
                )

            cur.execute("COMMIT;")
            print(f"✅ Successfully loaded {len(df)} rows into {target_table}")

        except Exception as e:
            cur.execute("ROLLBACK;")
            print("Error during Snowflake load:", e)
            raise
        finally:
            cur.close()
            conn.close()

    # -----------------------------
    # DAG Dependencies
    # -----------------------------
    extracted = extract()
    transformed = transform(extracted)
    load_to_snowflake(transformed)
