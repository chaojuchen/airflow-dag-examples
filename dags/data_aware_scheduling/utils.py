import functools
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

class TangramSQLExecutionOperator(SQLExecuteQueryOperator):
    template_fields = SQLExecuteQueryOperator.template_fields + ("prepend_str",)

    def __init__(self, *args, tangram_workspace, **kwargs):
        self.prepend_str = f'-- {{"job":{{"rn":{{"resourceType":{{"app":{{"group":"org.apache","name":"airflow"}},"name":"TaskInstance"}},"names":["{tangram_workspace}", "{{{{ti.dag_id}}}}","{{{{ti.run_id}}}}","{{{{ti.task_id}}}}"]}}}}}}'
        super().__init__(*args, **kwargs)

    def render_template_fields(self, context, jinja_env=None):
        def prepend_with_newline(prepend_str, sql):
            return prepend_str + '\n' + sql
        prepend_func = functools.partial(prepend_with_newline, self.prepend_str)
        # Use proper logging instead of print statements
        logger = logging.getLogger(__name__)
        
        logger.info(f"Prepending SQL with Tangram lineage info")
        logger.debug(f"Lineage info: {self.prepend_str}")
        logger.debug(f"Original SQL: {self.sql}")
        self.sql = prepend_func(self.sql) if isinstance(self.sql, str) else [prepend_func(s) for s in self.sql]


    def execute(self, context):
        print("Rendered SQL:", self.sql)
        return super().execute(context)