"""This module extracts information from Postgres database to Google Cloud Storage,
    massages the data and ultimately ingests into Google Big Query  """
import json
from datetime import datetime
import dateutil.parser
from airflow.models import DAG
from airflow.contrib.operators import postgres_to_gcs_operator
from airflow.operators import bash_operator
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import bigquery_operator
from airflow.operators import python_operator
from airflow import models


START_DATE = datetime(2019, 8, 11)
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

import datetime
import uuid
import shutil

uuid = str(uuid.uuid1())

current_timestamp = datetime.datetime.now()
current_time = current_timestamp
end_timestamp = datetime.datetime.now() - datetime.timedelta(minutes=15)
end_time = end_timestamp
temp_file = "/tmp/{}".format(uuid)

SQL_QUERY = """SELECT * FROM timestamp_table WHERE "Time_Stamp" BETWEEN '{}' and '{}'""".format(end_time, current_time)
SQL_BQ = "SELECT * FROM `key-conquest-250118.project_1.postgres_main_temp` union all select m.* FROM `key-conquest-250118.project_1.postgres_main` as m left join `key-conquest-250118.project_1.postgres_main_temp` as t on m.Emp_ID =  t.Emp_ID and m.Creation_Date =t.Creation_Date where t.Emp_ID is null"


VARS_POSTGRES_GBQ = json.loads(models.Variable.get('postgres_gbq_config'))

POSTGRES_CONN_ID = VARS_POSTGRES_GBQ["postgres_conn_id"]
GOOGLE_CLOUD_STORAGE_CONN_ID = VARS_POSTGRES_GBQ["google_cloud_storage_conn_id"]
BUCKET = VARS_POSTGRES_GBQ["bucket"]
SOURCE_OBJECTS = VARS_POSTGRES_GBQ["source_objects"]
SOURCE_FORMAT = VARS_POSTGRES_GBQ["source_format"]
CREATE_DISPOSITION = VARS_POSTGRES_GBQ["create_disposition"]
BIGQUERY_CONN_ID = VARS_POSTGRES_GBQ["bigquery_conn_id"]
FILE_NAME = VARS_POSTGRES_GBQ["file_name"]
OUT_FILE = VARS_POSTGRES_GBQ["out_file"]
DESTINATION_PROJECT_DATASET_TABLE_MAIN = VARS_POSTGRES_GBQ["destination_dataset_main"]


def date_format_conversion(**kwargs):
    """Date format converter"""
    input_file = kwargs.get("file_name")
    output_file = kwargs.get("out_file")
    with open(input_file, 'rU') as csv_file:

        for line in csv_file:
            line_dict = json.loads(line)
            creation_date_val = dateutil.parser.parse(line_dict['Creation_Date'])
            creation_date_val = creation_date_val.strftime('%Y-%m-%d')
            position_start_date = dateutil.parser.parse(line_dict['Position_Start_Date'])
            position_start_date = position_start_date.strftime('%Y-%m-%d')
            time_stamp = dateutil.parser.parse(line_dict['Time_Stamp'])
            time_stamp = time_stamp.strftime('%Y-%m-%d %H:%m:%S')
            line_dict['Creation_Date'] = creation_date_val
            line_dict['Position_Start_Date'] = position_start_date
            line_dict['Time_Stamp'] = time_stamp
            with open(output_file, 'a') as write_file:
                write_file.write(json.dumps(line_dict) + '\n')
    shutil.move(temp_file, OUT_FILE)


with DAG('postgres_time_stamp_file_to_gbq_final',
         schedule_interval=SCHEDULE_INTERVAL,
         default_args=DEFAULT_ARGS, catchup=False) as dag:

    POSTGRESS_TEST = postgres_to_gcs_operator.PostgresToGoogleCloudStorageOperator(
        task_id='postgres_timestamp_query_file',
        sql=SQL_QUERY,
        postgres_conn_id=POSTGRES_CONN_ID,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_STORAGE_CONN_ID,
        bucket=BUCKET,
        filename=FILE_NAME
    )

    CONVERT_DATE_GBQ_FORMAT = python_operator.PythonOperator(
        task_id='convert_date_format',
        python_callable=date_format_conversion,
        op_kwargs={"file_name": FILE_NAME, "out_file": OUT_FILE, "temp_file": temp_file}
    )

    MOVE_JSON_FILE_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='move_file_to_bq',
        bucket=BUCKET,
        source_objects=[SOURCE_OBJECTS],
        destination_project_dataset_table="{{0}_temp}".format(DESTINATION_PROJECT_DATASET_TABLE_MAIN),
        schema_fields=None,
        schema_object=None,
        autodetect=True,
        source_format=SOURCE_FORMAT,
        create_disposition=CREATE_DISPOSITION,
        write_disposition='WRITE_APPEND',
        google_cloud_storage_conn_id=GOOGLE_CLOUD_STORAGE_CONN_ID,
        bigquery_conn_id=BIGQUERY_CONN_ID
    )

    BQ_TO_BQ_MERGE = bigquery_operator.BigQueryOperator(
        task_id="Update_bq_table",
        sql=SQL_BQ,
        use_legacy_sql=False,
        destination_project_dataset_table=DESTINATION_PROJECT_DATASET_TABLE_MAIN,
        write_disposition='WRITE_EMPTY',
        create_disposition=CREATE_DISPOSITION,
    )
    GOODBYE_BASH = bash_operator.BashOperator(
        task_id='bye',
        bash_command='echo Goodbye.')

    POSTGRESS_TEST >> CONVERT_DATE_GBQ_FORMAT >> MOVE_JSON_FILE_BQ >> BQ_TO_BQ_MERGE >> GOODBYE_BASH
