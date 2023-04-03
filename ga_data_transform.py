from datetime import datetime, timedelta
import pandas as pd, pandas_gbq
from google.cloud import bigquery
from google.ads.googleads.client import GoogleAdsClient

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

################################## SETUP INITIAL VARIABLES ##################################
DAG = 'ga_data_transform'
LOCATION = 'europe-west3'
PROJECT = 'dtc-de-project-374319'
DATASET = 'ga_data'
GA_TABLE_RAW = 'ga_raw_data'
GA_TABLE_PROCESSED = 'ga_processed_data'
GA_PARQUET = 'ga_data.parquet'

################################# SET AIRFLOW DAG PARAMETRES ################################
default_args = {
    'owner': 'Composer',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}



################################## DAG CODE ##################################
with models.DAG(
    dag_id=f'{DATASET}_{DAG}',
    default_args=default_args,
    catchup=False,
    start_date=datetime.datetime(2023,4,1),
    schedule_interval='5 10,16,22 * * *',
) as dag:

    def ga_data_load() -> None:
        """Task to load data from BQ table to pandas dataframe. 
        Data gathered by usual SQL query.
        Data load to cloud stoarge in parquet format.
        
        Args:
            None.
        
        Returns:
            None.
        """
        
        query = """
                    SELECT
                        CAST(date AS DATE FORMAT 'YYYYMMDD'),
                        CAST(fullVisitorId AS STRING),
                        totals.transactions,
                        device.browser,
                        totals.timeOnSite,
                        channelGrouping,
                        geoNetwork.country,
                        totals.newVisits,
                        totals.visits
                    FROM 
                        `bigquery-public-data.google_analytics_sample.ga_sessions_*`
            """
        df = pd.read_gbq(query, project_id = PROJECT)
        df.to_parquet(GA_PARQUET)


    create_table = BigQueryInsertJobOperator(
        task_id='create_table',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query":
                f"""
                    CREATE OR REPLACE TABLE `{DATASET}.{GA_TABLE_RAW}`
                    (
                        date DATE,
                        fullVisitorId STRING,
                        transactions INTEGER,
                        browser STRING,
                        timeOnSite INTEGER,
                        channelGrouping STRING,
                        country STRING,
                        newVisits INTEGER,
                        visits INTEGER
                    )
                    CLUSTER BY country
                    PARTITION BY date
                """,
            "useLegacySql": False
            }
        }
    )


    def ga_data_upload() -> None:
        """Task to upload data from parquet file on storage to DWH. 
        Data downloaded from parquet to pandas dataframe then uploaded to BQ.
        
        Args:
            None.
        
        Returns:
            None.
        
        """
        df = pd.read_parquet(GA_PARQUET)
        bq_client = bigquery.Client()
        table = bq_client.get_table(GA_TABLE_RAW)
        generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]
        df.to_gbq(GA_TABLE_RAW, project_id=PROJECT, table_schema=generated_schema)


    dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="dbt_cloud_job_run",
        job_id=259988,
        check_interval=10,
        timeout=300,
    )


    ga_data_load = PythonOperator(
        task_id='ga_data_load',
        execution_timeout=datetime.timedelta(hours=1),
        python_callable=ga_data_load,
    )


    ga_data_upload = PythonOperator(
        task_id='ga_data_upload',
        execution_timeout=datetime.timedelta(hours=1),
        python_callable=ga_data_upload,
    )

    ga_data_load >> create_table >> ga_data_upload >> dbt_cloud_job_run