from etl_tools.copy_schema.copy_schema import ParallelCopySchema
from data_integration import pipelines
from data_integration.commands.sql import ExecuteSQL
from data_integration.pipelines import Task
from mara_db import dbs
import os

script_path = os.path.dirname(os.path.abspath(__file__))


def CopySchemas(root_pipeline: pipelines.Pipeline, target_db_alias: str = 'dwh-frontend',
                origin_db_alias: str = 'dwh-etl',
                max_number_of_parallel_jobs: int = 2):
    origin_db = dbs.db(origin_db_alias)
    target_db = dbs.db(target_db_alias)
    if origin_db.database != target_db.database or origin_db.host != target_db.host:
        root_pipeline.add_initial( ## initialization of frontend db
            Task(id="initialize_frontend", description="Initializes the frontend",
                 commands=[ExecuteSQL(sql_file_name=script_path + '/initialize_frontend.sql',
                                      echo_queries=True,
                                      db_alias=target_db_alias)]))
        for pipeline in root_pipeline.nodes.values(): ## insert schema copy into last job
            if "Schema" in pipeline.labels:
                final_commands = pipeline.final_node.commands
                pipeline.remove(pipeline.final_node)
                pipeline.add_final(
                    ParallelCopySchema(
                        id="schema_copy_" + pipeline.labels["Schema"],
                        description="copy schema to frontend",
                        schema_name=pipeline.labels["Schema"] + '_next',
                        origin_db_alias=origin_db_alias, target_db_alias=target_db_alias,
                        max_number_of_parallel_tasks=max_number_of_parallel_jobs,
                        commands_after=final_commands))
