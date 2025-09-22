from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging

class TangramSQLExecutionOperator(SQLExecuteQueryOperator):
    # Only sql needs to be a template field since we concatenate in __init__
    template_fields = SQLExecuteQueryOperator.template_fields

    def __init__(self, *args, tangram_workspace, **kwargs):
        # Create the prepend string with Jinja variables
        prepend_str = f'-- {{"job":{{"rn":{{"resourceType":{{"app":{{"group":"org.apache","name":"airflow"}},"name":"TaskInstance"}},"names":["{tangram_workspace}", "{{{{ti.dag_id}}}}","{{{{ti.run_id}}}}","{{{{ti.task_id}}}}"]}}}}}}'
        
        # Concatenate prepend_str with sql BEFORE calling super().__init__
        # This way the combined string will be rendered by Airflow's templating
        if "sql" in kwargs:
            if isinstance(kwargs["sql"], str):
                kwargs["sql"] = prepend_str + '\n' + kwargs["sql"]
            elif isinstance(kwargs["sql"], list):
                kwargs["sql"] = [prepend_str + '\n' + s for s in kwargs["sql"]]
        
        super().__init__(*args, **kwargs)

    def execute(self, context):
        logger = logging.getLogger(__name__)
        logger.info("Executing SQL with rendered Jinja variables")
        print("Rendered SQL:", self.sql)
        return super().execute(context)