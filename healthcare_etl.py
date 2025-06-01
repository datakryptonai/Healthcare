from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.ingestion import load_ehr_data, load_billing_data, load_lab_results
from src.mdm import deterministic_match, apply_survivorship
from src.quality import validate_patient_ids, validate_icd10_codes
from pyspark.sql import SparkSession

def run_etl():
    spark = SparkSession.builder.appName("HealthcareETL").getOrCreate()

    ehr_df = load_ehr_data(spark, "/data/raw/ehr_data.csv")
    billing_df = load_billing_data(spark, "/data/raw/billing_data.csv")
    lab_df = load_lab_results(spark, "/data/raw/lab_results.csv")

    matched_df = deterministic_match(ehr_df, billing_df, keys=["patient_id", "date_of_birth"])

    priorities = {
        "phone": ["ehr", "billing"],
        "address": ["ehr", "billing"]
    }
    golden_df = apply_survivorship(matched_df, priorities)

    validate_patient_ids(golden_df)
    validate_icd10_codes(golden_df)

    golden_df.write.mode("overwrite").parquet("/data/trusted/golden_patients.parquet")

default_args = {"start_date": datetime(2025, 6, 1), "retries": 1}

with DAG(
    "healthcare_patient_360_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    etl_task = PythonOperator(task_id="run_healthcare_etl", python_callable=run_etl)