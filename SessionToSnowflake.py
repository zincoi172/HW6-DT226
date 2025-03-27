from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def create_stage():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        sql = """
        CREATE OR REPLACE STAGE raw.blob_stage
        URL = 's3://s3-geospatial/readonly/'
        FILE_FORMAT = (
            TYPE = CSV,
            SKIP_HEADER = 1,
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        );
        """
        cur.execute(sql)
        cur.execute("COMMIT;")
        print("Stage created successfully.")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Failed to create stage.")
        raise e


@task
def load_data():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")

        # Load user_session_channel
        copy_user_sql = """
        COPY INTO raw.user_session_channel
        FROM @raw.blob_stage/user_session_channel.csv
        ON_ERROR = 'CONTINUE';
        """
        cur.execute(copy_user_sql)

        # Load session_timestamp
        copy_session_sql = """
        COPY INTO raw.session_timestamp
        FROM @raw.blob_stage/session_timestamp.csv
        ON_ERROR = 'CONTINUE';
        """
        cur.execute(copy_session_sql)

        cur.execute("COMMIT;")
        print("Data loaded successfully.")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Failed to load data: {e}")
        raise e


default_args = {
    'start_date': datetime(2025, 3, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='SessionToSnowflake',
    default_args=default_args,
    schedule_interval='*/45 2 * * *',
    catchup=False,
    tags=['ETL', 'snowflake'],
    description='DAG to load user session data into Snowflake from S3'
) as dag:

    create_stage_task = create_stage()
    load_data_task = load_data()

    create_stage_task >> load_data_task
