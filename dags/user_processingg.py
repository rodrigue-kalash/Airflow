from airflow import DAG
#Postgres
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#Http
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
#Python
from airflow.operators.python_operator import PythonOperator


import json
from pandas import json_normalize
from datetime import datetime

# To enter docker use command : docker exec -it <container_id> /bin/bash
#inside docker to test the dag use command : airflow dags test <dag_id> <task_id> <date>
# To exit docker use ctrl + D


def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user['results'][0]
    processed_user = json_normalize({
        "firstname": user["name"]["first"],
        "lastname": user["name"]["last"],
        "country": user["location"]["country"],
        "username": user["login"]["username"],
        "password": user["login"]["password"],
        "email": user["email"]
    })
    processed_user.to_csv("/tmp/processed_user.csv", index=None, header=False)

def _store_user():
    hook=PostgresHook(postgres_conn_id="postgres")
    hook.copy_expert(
        sql="COPY users FROM STDIN WITH DELIMITER as ','",
        filename="/tmp/processed_user.csv"
    )


# Create DAG
with DAG(
    "user_processingg",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    # Define tasks/operators
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS users(
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL 
            );
        """
    )
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="user_api",
        endpoint="api/"
    )
    extract_user = SimpleHttpOperator(
        task_id="extract_user",
        http_conn_id="user_api",
        endpoint="api/",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
    process_user = PythonOperator(
        task_id="process_user",
        python_callable=_process_user
    )
    store_user = PythonOperator(
        task_id="store_user",
        python_callable=_store_user
    )
    # Define dependencies
    # create_table >> is_api_available
    # is_api_available >> extract_user
    # extract_user >> process_user 
    # process_user>> store_user
    create_table >> is_api_available >> extract_user >> process_user >> store_user