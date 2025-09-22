from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

class SQLExecuteQueryOperatorWithLineage(SQLExecuteQueryOperator):
    def __init__(self, *args, tangram_workspace, **kwargs):
        super().__init__(*args, **kwargs)
        prepend_str = f'-- {{"job":{{"rn":{{"resourceType":{{"app":{{"group":"org.apache","name":"airflow"}},"name":"TaskInstance"}},"names":["{tangram_workspace}", "{{{{ti.dag_id}}}}","{{{{ti.run_id}}}}","{{{{ti.task_id}}}}"]}}}}}}\n'
        print(f"Prepending SQL with Tangram lineage info: {prepend_str}")
        if isinstance(self.sql, str):
            self.sql = prepend_str + self.sql
        elif isinstance(self.sql, list):
            self.sql = [prepend_str + s for s in self.sql]