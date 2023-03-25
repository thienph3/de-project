from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = "create_restaurant_table_dag"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_restaurant_table_task",
        postgres_conn_id="dwh-conn-id",
        sql="""
            CREATE TABLE restaurants(
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR,
                stars VARCHAR,
                review VARCHAR,
                price VARCHAR,
                note VARCHAR, 
                open_time VARCHAR,
                restaurant_type VARCHAR, 
                address VARCHAR, 
                ward VARCHAR, 
                district VARCHAR, 
                city VARCHAR
            );
          """,
    )
