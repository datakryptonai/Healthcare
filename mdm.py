from pyspark.sql import DataFrame
from pyspark.sql.functions import when, coalesce, col

def deterministic_match(df1: DataFrame, df2: DataFrame, keys: list):
    joined = df1.join(df2, keys, "inner")
    return joined

def probabilistic_match(df1: DataFrame, df2: DataFrame):
    pass

def apply_survivorship(df: DataFrame, priorities: dict):
    for attr, sources in priorities.items():
        df = df.withColumn(attr, coalesce(*[col(f"{src}.{attr}") for src in sources]))
    return df