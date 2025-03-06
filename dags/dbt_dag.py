from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.hooks.base import BaseHook
import json
import os
from datetime import datetime

# Get Snowflake credentials
snowflake_conn = BaseHook.get_connection('snowflake_default')

dbt_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=f"{os.environ['AIRFLOW_HOME']}/dags/dbt/medicare_dbt",
        env_vars={
            "snowflake_password": snowflake_conn.password,
        },
    ),
    profile_config=ProfileConfig(
        profile_name="medicare_analytics_dbt",
        target_name="dev",
        profiles_yml_filepath=f"{os.environ['AIRFLOW_HOME']}/dags/dbt/medicare_dbt/profiles.yml",
    ),
    execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_USER_HOME_DIR']}/.local/bin/dbt",
        ),
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_dag",
    # default_args={"retries": 2},
)
