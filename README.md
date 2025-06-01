# Healthcare Patient 360 Data Platform

This project delivers a unified and trusted patient data platform integrating clinical, administrative, and billing records using MDM principles.

## Features

- Scalable PySpark data ingestion pipelines
- Patient identity reconciliation and golden records creation
- dbt modular transformations and business logic
- Automated data quality with Great Expectations
- Workflow orchestration with Apache Airflow
- Visualization-ready trusted patient data

## Contact

Email: debjyoti@datakrypton.ai  
Website: [www.datakrypton.ai](http://www.datakrypton.ai)

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Trigger Airflow DAG:

```bash
airflow dags trigger healthcare_patient_360_etl
```

3. Explore trusted data layer and integrate with BI tools.

## Author

Debjyoti Kar — Data Architect | Data Engineer | Data Analyst