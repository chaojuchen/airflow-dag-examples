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

import os

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

dag_id = os.path.basename(__file__).replace(".py", "")

with DAG(
    dag_id=dag_id,
    start_date=pendulum.datetime(2022, 5, 1, tz="UTC"),
    schedule=None,
    tags=["example"],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="group1") as tg1:
        t1 = EmptyOperator(task_id="task1")

        # Define sub task group
        with TaskGroup(group_id="group1_1") as tg1_1:
            t2 = EmptyOperator(task_id="task2")
            t3 = EmptyOperator(task_id="task3")

            t2 >> t3

        with TaskGroup(group_id="group1_2") as tg1_2:
            for i in range(1, 3):
                tn = EmptyOperator(task_id=f"task_tg1_2_{i}")

            t3 >> tn

        t1 >> tg1_1 >> tg1_2

    end = EmptyOperator(task_id="end")

    start >> tg1 >> end
