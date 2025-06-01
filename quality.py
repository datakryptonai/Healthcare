import great_expectations as ge

def validate_patient_ids(df):
    ge_df = ge.from_pandas(df.toPandas())
    result = ge_df.expect_column_values_to_not_be_null("patient_id")
    assert result["success"], "Null patient IDs found!"

def validate_icd10_codes(df):
    ge_df = ge.from_pandas(df.toPandas())
    result = ge_df.expect_column_values_to_match_regex("diagnosis_code", r"^[A-Z]\d{2}(\.\d{1,4})?$")
    assert result["success"], "Invalid ICD-10 diagnosis codes found!"