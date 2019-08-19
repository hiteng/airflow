"""
This module extracts information from sftp servers as CSV files and
is loaded into Big Query once the data is massaged.
"""
import json
from datetime import datetime
from airflow.models import DAG
from airflow.contrib.operators import sftp_operator
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import bigquery_operator
from airflow import models

VARS_SFTP_GBQ = json.loads(models.Variable.get('sftp_gcs_gbq_config'))

SSH_CONN_ID = VARS_SFTP_GBQ["ssh_conn_id"]
GOOGLE_CLOUD_STORAGE_CONN_ID = VARS_SFTP_GBQ["google_cloud_storage_conn_id"]
LOCAL_FILEPATH = VARS_SFTP_GBQ["local_filepath"]
REMOTE_FILEPATH = VARS_SFTP_GBQ["remote_filepath"]
OPERATION = VARS_SFTP_GBQ["operation"]
BUCKET = VARS_SFTP_GBQ["bucket"]
SOURCE_OBJECTS = VARS_SFTP_GBQ["source_objects"]
BIGQUERY_CONN_ID = VARS_SFTP_GBQ["bigquery_conn_id"]
SOURCE_FORMAT = VARS_SFTP_GBQ["source_format"]
CREATE_DISPOSITION = VARS_SFTP_GBQ["create_disposition"]
DESTINATION_PROJECT_DATASET_TABLE_MAIN = VARS_SFTP_GBQ["destination_dataset_main"]

SQL = "SELECT * FROM `inspired-aria-236403.project_1.sftp_main_temp` union all select m.* FROM `inspired-aria-236403.project_1.sftp_main` as m left join `inspired-aria-236403.project_1.sftp_main_temp` as t on m.Emp_ID =  t.Emp_ID and m.Creation_Date =t.Creation_Date where t.Emp_ID is null"


START_DATE = datetime(2019, 8, 9)
SCHEDULE_INTERVAL = '*/15 * * *'
DEFAULT_ARGS = {
    'owner': 'Hitendra',
    'max_active_runs': 1,
    'depends_on_past': False,
    'start_date': START_DATE,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}


with DAG('sftp_csv_to_gbq',
         schedule_interval=SCHEDULE_INTERVAL,
         default_args=DEFAULT_ARGS, catchup=False) as dag:

    MOVE_SFTP_FILE_TO_GCS = sftp_operator.SFTPOperator(
        task_id="move_sftp_file_to_gcs",
        ssh_conn_id=SSH_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_STORAGE_CONN_ID,
        local_filepath=LOCAL_FILEPATH,
        remote_filepath=REMOTE_FILEPATH,
        operation=OPERATION,
        confirm=True
    )

    MOVE_GCS_FILE_TO_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="move_gcs_file_to_bq",
        bucket=BUCKET,
        source_objects=[SOURCE_OBJECTS],
        destination_project_dataset_table="{{0}_temp}".format(DESTINATION_PROJECT_DATASET_TABLE_MAIN),
        skip_leading_rows=1,
        autodetect=True,
        source_format=SOURCE_FORMAT,
        create_disposition=CREATE_DISPOSITION,
        write_disposition='WRITE_APPEND',
        google_cloud_storage_conn_id=GOOGLE_CLOUD_STORAGE_CONN_ID,
        bigquery_conn_id=BIGQUERY_CONN_ID
    )

    BQ_TO_BQ_MERGING = bigquery_operator.BigQueryOperator(
        task_id="gbq_merging_tables",
        sql=SQL,
        use_legacy_sql=False,
        destination_project_dataset_table=DESTINATION_PROJECT_DATASET_TABLE_MAIN,
        write_disposition='WRITE_EMPTY',
        create_disposition=CREATE_DISPOSITION,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_STORAGE_CONN_ID,
        bigquery_conn_id=BIGQUERY_CONN_ID,
    )

    MOVE_SFTP_FILE_TO_GCS >> MOVE_GCS_FILE_TO_BQ >> BQ_TO_BQ_MERGING
