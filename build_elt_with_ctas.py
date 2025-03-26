from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

"""
This pipeline assumes that there are two other tables in your Snowflake DB:
 - USER_DB_BISON.raw.user_session_channel
 - USER_DB_BISON.raw.session_timestamp

Output:
 - USER_DB_BISON.analytics.session_summary

"""

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_ctas(table, select_sql, primary_key=None):
    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        sql = f"CREATE OR REPLACE TABLE {table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # Check for duplicate 
        if primary_key is not None:
            sql = f"SELECT {primary_key}, COUNT(1) AS cnt FROM {table} GROUP BY 1 ORDER BY 2 DESC LIMIT 1"
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")
            
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise


with DAG(
    dag_id='BuildSummary',
    start_date = datetime(2025, 3, 24),
    schedule_interval = '45 2 * * *',  # Daily at 2:45 AM
    catchup=False,
    tags = ['ELT']
) as dag:

    table = "USER_DB_BISON.analytics.session_summary"

    select_sql = """
    SELECT u.*, s.ts
    FROM USER_DB_BISON.raw.user_session_channel u
    JOIN USER_DB_BISON.raw.session_timestamp s 
        ON u.sessionId = s.sessionId
    """

    run_ctas(table, select_sql, primary_key = 'sessionId')