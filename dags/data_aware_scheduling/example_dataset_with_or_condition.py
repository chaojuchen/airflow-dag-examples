#
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

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset

example_asset1 = Asset("s3://dataset1/or.csv")
example_asset2 = Asset("s3://dataset2/or.csv")

with DAG(
    dag_id="example_dataset_producer_with_or_condition",
    start_date=pendulum.datetime(2024, 11, 1, tz="UTC"),
    schedule=None,
    tags=["asset"],
):
    # `example_asset1` may be updated by this upstream producer task.
    wait5 = BashOperator(
        task_id="wait5",
        bash_command="sleep 5",
        outlets=[example_asset1],
    )

    # `example_dataset_consumer_with_or_condition` DAG is executed before this `wait15` task is completed.
    wait15 = BashOperator(
        task_id="wait15",
        bash_command="sleep 15",
        outlets=[example_asset2],
    )

    wait5 >> wait15


# This DAG is triggered when `wait5` task of `example_producer` DAG is completed successfully.
with DAG(
    dag_id="example_dataset_consumer_with_or_condition",
    start_date=pendulum.datetime(2024, 11, 1, tz="UTC"),
    schedule=(example_asset1 | example_asset2),
    tags=["asset"],
):
    EmptyOperator(task_id="consumer")
