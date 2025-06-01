from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_ehr_data(spark: SparkSession, path: str):
    df = spark.read.csv(path, header=True, inferSchema=True)
    df_clean = df.dropDuplicates().filter(col("patient_id").isNotNull())
    return df_clean

def load_billing_data(spark: SparkSession, path: str):
    df = spark.read.csv(path, header=True, inferSchema=True)
    df_clean = df.dropDuplicates().filter(col("patient_id").isNotNull())
    return df_clean

def load_lab_results(spark: SparkSession, path: str):
    df = spark.read.csv(path, header=True, inferSchema=True)
    df_clean = df.dropDuplicates().filter(col("patient_id").isNotNull())
    return df_clean