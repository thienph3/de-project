from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = "clean_restaurant_table_dag"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="clean_restaurant_table_task",
        postgres_conn_id="dwh-conn-id",
        sql="""
            CREATE TABLE clean_restaurants
            AS
            SELECT id + 1 AS id
                 , trim("name") AS "name"
                 , CASE WHEN stars IN ('None', '') THEN null ELSE stars END::float8 AS stars
                 , CASE WHEN review IN ('None', '') THEN null ELSE translate(review, '(),', '') END::int4 AS review
                 , CASE price WHEN 'None' THEN 0 ELSE length(price) END as price
                 , note LIKE '%Dine-in%' AS is_dine_in
                 , note LIKE '%Takeout%' AS is_takeout
                 , note LIKE '%Delivery%' AS is_delivery
                 , note LIKE '%No-contact delivery%' AS is_no_contact_delivery
                 , note LIKE '%Drive-through%' AS is_drive_through
                 , note LIKE '%Curbside pickup%' AS is_curbside_pickup
                 , note LIKE '%No delivery%' AS is_no_delivery
                 , note LIKE '%No takeout%' AS is_no_takeout
                 , note LIKE '%No dine-in%' AS is_no_dine_in
                 , note LIKE '%In-store pickup%' AS is_in_store_pickup
                 , open_time
                 , CASE 
                     WHEN trim(restaurant_type) IN ('None', 'Local government office', 'Place of worship', 'Post office', 'Market', 'Federal police', 'Gò Vấp', 'City government office', 'Yakiniku','Phú Nhuận') then null
                     ELSE trim(restaurant_type)
                   END AS restaurant_type
                 , TRIM(NULLIF(address, 'None')) as address
                 , ward
                 , district
                 , city
              FROM restaurants;
          """,
    )
