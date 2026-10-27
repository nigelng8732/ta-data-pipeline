import logging

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="ta_connection_check",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kuala_Lumpur"),
    schedule=None,
    catchup=False,
    tags=["ta", "setup"],
)
def ta_connection_check():

    @task
    def check_snowflake():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id="ta_snowflake")

        result = hook.get_first("""
            select
                current_role(),
                current_warehouse(),
                current_database(),
                current_schema()
        """)

        logging.info(
            "Role=%s | Warehouse=%s | Database=%s | Schema=%s",
            *result,
        )

        expected = (
            "TA_DATA_ENGINEER",
            "TA_DATA_PROJECT_WH",
            "TA_DATA_DB",
            "STAGING",
        )

        if tuple(result) != expected:
            raise ValueError(
                f"Unexpected connection settings: {result}. Expected: {expected}"
            )

    check_snowflake()


ta_connection_check()