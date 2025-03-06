from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
from datetime import timedelta 

@dag(
    description="A test DAG for PySpark job",
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 2, 14, tz="UTC"),
    catchup=False,
)
def test_pyspark():

    @task(task_id="start")
    def start():
        print("Jobs started")
    
    pyspark_task = SparkSubmitOperator(
        task_id="submit_spark_job",
        application="/opt/airflow/dags/scripts/wordcountjob.py",
        conn_id="spark_default",
        conf={
            "spark.driver.bindAddress": "0.0.0.0", # this works, it tells spark to bind to all interfaces (recommended as it works no matter what the airflow worker's IP address is)
            # "spark.driver.bindAddress": "172.18.0.5", # this works, it tells spark to bind to the airflow worker's ip address
            # "spark.driver.host": "172.18.0.5", # this works, it tells spark to bind to the airflow worker's ip address, only if the airflow worker's IP address is known
            # "spark.driver.host": "medicare-analytics-airflow-worker-1", # this doesn't work because the driver (airflow worker container) doesn't know this name refers to itself - this is only known by other containers
            # "spark.driver.host": "9b88980b9790", # this doesn't work because while this is the internal hostname that the driver (airflow worker container) knows, other containers don't know this name
        }
    )


    @task(task_id="end")
    def end():
        print("Jobs ended")

    
    start() >> pyspark_task >> end()

test_pyspark()
