import functools
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

class TangramSQLExecutionOperator(SQLExecuteQueryOperator):
    def __init__(self, *args, tangram_workspace, **kwargs):
        super().__init__(*args, **kwargs)

        def prepend_with_newline(prepend_str, sql):
            return prepend_str + '\n' + f"{sql}"

        prepend_str = f'-- {{"job":{{"rn":{{"resourceType":{{"app":{{"group":"org.apache","name":"airflow"}},"name":"TaskInstance"}},"names":["{tangram_workspace}", "{{{{ti.dag_id}}}}","{{{{ti.run_id}}}}","{{{{ti.task_id}}}}"]}}}}}}'
        prepend_func = functools.partial(prepend_with_newline, prepend_str)
        print(f"Prepending SQL with Tangram lineage info: {prepend_str}")

        
        print(f"Original SQL: {self.sql}")
        self.sql = prepend_func(self.sql) if isinstance(self.sql, str) else [prepend_func(s) for s in self.sql]