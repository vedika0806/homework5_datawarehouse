# Alpha Vantage → Snowflake ETL (Airflow DAG)

This repository contains an **Apache Airflow DAG** that extracts daily stock price data for **TSLA** from the [Alpha Vantage API](https://www.alphavantage.co/) and loads it into **Snowflake**.

---

## Overview

**DAG ID:** `alpha_vantage_to_snowflake_etl`  
**Schedule:** Daily (`@daily`)  
**Purpose:** Automate ETL from Alpha Vantage → Snowflake warehouse.

### Steps:
1. **Extract** – Fetch latest TSLA stock data using Alpha Vantage free API  
2. **Transform** – Clean and convert JSON to numeric types  
3. **Load** – Insert into Snowflake table (`RAW.STOCK_PRICES`)

---

## Project Structure
