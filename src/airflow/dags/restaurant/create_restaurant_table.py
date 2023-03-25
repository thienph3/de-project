# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import datetime
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# [START postgres_operator_howto_guide]


# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

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
