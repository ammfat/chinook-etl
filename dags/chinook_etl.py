""" Test PostgreSQL connection """

from datetime import timedelta
import json
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs \
    import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery \
    import GCSToBigQueryOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup

from airflow.models.variable import Variable

BASE_PATH = Variable.get("BASE_PATH")
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID")
GOOGLE_CLOUD_CONN_ID = Variable.get("GOOGLE_CLOUD_CONN_ID")
BQ_DATASET_ID = Variable.get("BQ_DATASET_ID")
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET_NAME")

default_args = {
    'owner': 'ammfat',
}

warehouse_tables = [
    'dim_songs',
    'dim_transactions',
    'fact_artist_revenues'
]


def _get_dag_config(ds):
    """ Get config """

    return {
        "BASE_PATH": BASE_PATH,
        "POSTGRES_CONN_ID": POSTGRES_CONN_ID,
        "GOOGLE_CLOUD_CONN_ID": GOOGLE_CLOUD_CONN_ID,
        "BQ_DATASET_ID": BQ_DATASET_ID,
        "GCS_BUCKET_NAME": GCS_BUCKET_NAME,
        "ds": ds,
        "dag_tz": dag.timezone,
    }


def _get_pg_query(table_name):
    """ Get SQL query from file """

    with open(
        f"{BASE_PATH}/models/sql/{table_name}.sql", "r", encoding="utf-8"
    ) as query:
        return query.read()


def _get_bq_schema(table_name):
    """ Get BigQuery schema from file """

    with open(
        f"{BASE_PATH}/models/schemas/{table_name}.json", "r", encoding="utf-8"
    ) as schema:
        bq_schema = json.load(schema)

    return bq_schema['schema_fields'], bq_schema['time_partitioning']


with DAG(
    dag_id='chinook_etl',
    default_args=default_args,
    start_date=pendulum.datetime(2008, 12, 1, 5),
    end_date=pendulum.datetime(2013, 12, 1, 5),
    schedule_interval='@monthly',
    description='ETL to load Chinook from PostgreSQL to BigQuery',
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Start"',
    )

    get_dag_config = PythonOperator(
        task_id='get_dag_config',
        python_callable=_get_dag_config,
    )

    postgres_sensor = SqlSensor(
        task_id='postgres_sensor',
        conn_id=POSTGRES_CONN_ID,
        sql="""SELECT 1 AS check_row FROM "Invoice" i
                WHERE date_trunc('month', "InvoiceDate") = '{{ ds }}'""",
        poke_interval=3,
        timeout=6,
        mode='reschedule',
        soft_fail=True,
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "End"',
    )

    table_task_group = {}

    for table in warehouse_tables:
        with TaskGroup(group_id=f'load_{table}') as load_group:
            postgres_to_gcs = PostgresToGCSOperator(
                task_id=f'postgres_to_gcs_{table}',
                postgres_conn_id=POSTGRES_CONN_ID,
                gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
                sql=_get_pg_query(table),
                bucket=GCS_BUCKET_NAME,
                filename=f'{table}/{{{{ ds_nodash }}}}.csv',
                retries=3,
                retry_delay=timedelta(seconds=30),
            )

            bq_schema_fields, bq_time_partitioning = _get_bq_schema(table)

            gcs_to_bigquery = GCSToBigQueryOperator(
                task_id=f'gcs_to_bigquery_{table}',
                gcp_conn_id=GOOGLE_CLOUD_CONN_ID,
                bucket=GCS_BUCKET_NAME,
                source_objects=[f'{table}/{{{{ ds_nodash }}}}.csv'],
                destination_project_dataset_table=f"{BQ_DATASET_ID}.{table}",
                source_format='NEWLINE_DELIMITED_JSON',
                schema_fields=bq_schema_fields,
                time_partitioning=bq_time_partitioning,
                write_disposition='WRITE_APPEND',
                retries=3,
                retry_delay=timedelta(seconds=30),
            )

            postgres_to_gcs >> gcs_to_bigquery

        table_task_group[table] = load_group

    start >> get_dag_config >> postgres_sensor

    for table, table_task_group in table_task_group.items():
        postgres_sensor >> table_task_group >> end

if __name__ == "__main__":
    dag.cli()
