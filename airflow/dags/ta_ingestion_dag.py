import logging
from datetime import date

from cosmos import (
    DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig,
)
from cosmos.constants import LoadMode, TestBehavior, SourceRenderingBehavior
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

import pendulum
from airflow.sdk import dag, task, Param, get_current_context


@dag(
    dag_id="ta_ingestion",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kuala_Lumpur"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "snapshot_date": Param(
            "2026-10-09",
            type="string",
            format="date",
            description="Snapshot date to load, in YYYY-MM-DD format",
        )
    },
    tags=["ta", "ingestion"],
)
def ta_ingestion():

    @task
    def check_snapshot_files():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        context = get_current_context()
        snapshot_date = context["params"]["snapshot_date"]

        # Validate before inserting the date into a stage path.
        parsed_date = date.fromisoformat(snapshot_date)
        if parsed_date.isoformat() != snapshot_date:
            raise ValueError("snapshot_date must use YYYY-MM-DD format")

        hook = SnowflakeHook(snowflake_conn_id="ta_snowflake")

        stage_path = (
            f"@TA_DATA_DB.RAW.TA_S3_STAGE/{snapshot_date}/"
        )

        files = hook.get_records(f"LIST {stage_path}")

        expected = {
            "job_requisitions.csv",
            "applications.csv",
            "recruiters.csv",
            "application_status_audit.csv",
        }

        # Check the exact snapshot directory, excluding nested folders.
        found = {
            filename
            for filename in expected
            if any(
                row[0].endswith(f"/{snapshot_date}/{filename}")
                for row in files
            )
        }

        missing = expected - found

        if missing:
            raise FileNotFoundError(
                f"Missing files for {snapshot_date}: {sorted(missing)}"
            )

        logging.info(
            "Snapshot %s: all four required files found: %s",
            snapshot_date,
            sorted(found),
        )

        return snapshot_date

    @task
    def load_snapshot_into_raw(snapshot_date: str):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        # Expected RAW counts for our two mock snapshots.
        expected_counts = {
            "2026-10-08": {
                "JOB_REQUISITIONS": 20,
                "APPLICATIONS": 101,
                "RECRUITERS": 6,
                "APPLICATION_STATUS_AUDIT": 348,
            },
            "2026-10-09": {
                "JOB_REQUISITIONS": 20,
                "APPLICATIONS": 102,
                "RECRUITERS": 5,
                "APPLICATION_STATUS_AUDIT": 357,
            },
        }

        if snapshot_date not in expected_counts:
            raise ValueError(
                f"No expected counts configured for {snapshot_date}"
            )

        # Column order must match each CSV.
        tables = {
            "JOB_REQUISITIONS": (
                "job_requisitions.csv",
                [
                    "JOB_REQ_ID", "JOB_TITLE", "JOB_GRADE",
                    "BUSINESS_AREA", "COUNTRY", "RECRUITER_ID",
                    "REQUISITION_STATUS", "APPROVED_DATE",
                    "CLOSED_DATE", "SNAPSHOT_DATE",
                ],
            ),
            "APPLICATIONS": (
                "applications.csv",
                [
                    "APPLICATION_ID", "JOB_REQ_ID", "CANDIDATE_ID",
                    "CANDIDATE_NAME", "APPLICATION_STATUS",
                    "CANDIDATE_COUNTRY", "APPLICATION_DATE",
                    "SNAPSHOT_DATE",
                ],
            ),
            "RECRUITERS": (
                "recruiters.csv",
                [
                    "RECRUITER_ID", "RECRUITER_NAME",
                    "RECRUITER_ORG_UNIT", "SNAPSHOT_DATE",
                ],
            ),
            "APPLICATION_STATUS_AUDIT": (
                "application_status_audit.csv",
                [
                    "APPLICATION_ID", "APPLICATION_STATUS",
                    "CREATED_DATE", "SNAPSHOT_DATE",
                ],
            ),
        }

        metadata_columns = [
            "SOURCE_FILENAME",
            "SOURCE_ROW_NUMBER",
            "SOURCE_CONTENT_KEY",
            "INGESTED_AT",
        ]

        hook = SnowflakeHook(snowflake_conn_id="ta_snowflake")

        for table, (filename, columns) in tables.items():
            target_columns = ", ".join(columns + metadata_columns)

            csv_fields = ", ".join(
                f"s.${i}" for i in range(1, len(columns) + 1)
            )

            copy_sql = f"""
                COPY INTO TA_DATA_DB.RAW.{table} (
                    {target_columns}
                )
                FROM (
                    SELECT
                        {csv_fields},
                        METADATA$FILENAME,
                        METADATA$FILE_ROW_NUMBER,
                        METADATA$FILE_CONTENT_KEY,
                        METADATA$START_SCAN_TIME
                    FROM
                        @TA_DATA_DB.RAW.TA_S3_STAGE/{snapshot_date}/ s
                )
                FILES = ('{filename}')
                ON_ERROR = 'ABORT_STATEMENT'
                FORCE = FALSE
            """

            results = hook.get_records(copy_sql)
            logging.info("%s COPY result: %s", table, results)

            row = hook.get_first(
                f"""
                    SELECT COUNT(*)
                    FROM TA_DATA_DB.RAW.{table}
                    WHERE SNAPSHOT_DATE = %s
                """,
                parameters=(snapshot_date,),
            )

            actual = row[0]
            expected = expected_counts[snapshot_date][table]

            if actual != expected:
                raise ValueError(
                    f"{table}: expected {expected} rows "
                    f"for {snapshot_date}, found {actual}"
                )

            logging.info(
                "%s: verified %s rows for %s",
                table,
                actual,
                snapshot_date,
            )

        return snapshot_date

    profile_config = ProfileConfig(
        profile_name="ta_dbt",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="ta_snowflake",
            profile_args={
                "database": "TA_DATA_DB",
                "schema": "STAGING",
                "warehouse": "TA_DATA_PROJECT_WH",
                "role": "TA_DATA_ENGINEER",
                "threads": 2,
            },
        ),
    )

    build_dbt = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dbt/ta_dbt",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
            should_detach_multiple_parents_tests=True,
        ),
        operator_args={
            "vars": {
                "ta_snapshot_date": (
                    "{{ ti.xcom_pull(task_ids='load_snapshot_into_raw') }}"
                ),
            },
            "append_env": True,
        },
    )

    @task
    def verify_reporting(snapshot_date: str):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        expected_counts = {
            "2026-10-08": {
                "DIM_RECRUITERS": 5,
                "DIM_REQUISITIONS": 17,
                "FCT_APPLICATIONS": 81,
                "REJECTED_APPLICATIONS": 19,
                "FCT_APPLICATION_STATUS_EVENTS": 278,
            },
            "2026-10-09": {
                "DIM_RECRUITERS": 5,
                "DIM_REQUISITIONS": 20,
                "FCT_APPLICATIONS": 102,
                "REJECTED_APPLICATIONS": 0,
                "FCT_APPLICATION_STATUS_EVENTS": 357,
            },
        }

        hook = SnowflakeHook(snowflake_conn_id="ta_snowflake")

        for table, expected in expected_counts[snapshot_date].items():
            total, matching_date = hook.get_first(
                f"""
                    SELECT
                        COUNT(*),
                        COALESCE(
                            COUNT_IF(SNAPSHOT_DATE = TO_DATE(%s)),
                            0
                        )
                    FROM TA_DATA_DB.STAGING.{table}
                """,
                parameters=(snapshot_date,),
            )

            if total != expected or matching_date != expected:
                raise ValueError(
                    f"{table}: expected {expected} rows for "
                    f"{snapshot_date}; found {total} total rows "
                    f"and {matching_date} matching the date"
                )

            logging.info(
                "%s: verified %s rows for %s", table, total, snapshot_date
            )

    snapshot_date = check_snapshot_files()
    loaded_snapshot = load_snapshot_into_raw(snapshot_date)
    reporting_check = verify_reporting(loaded_snapshot)

    loaded_snapshot >> build_dbt >> reporting_check


ta_ingestion()
