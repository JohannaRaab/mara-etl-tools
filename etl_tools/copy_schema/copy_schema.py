from data_integration import config, pipelines
from data_integration.commands import bash, sql
from mara_page import _, html

from mara_db import dbs
import mara_db


class ParallelCopySchema(pipelines.ParallelTask):
    def __init__(self, id: str, description: str,
                 max_number_of_parallel_tasks: int = None,
                 commands_before: [pipelines.Command] = None, commands_after: [pipelines.Command] = None,
                 echo_queries: bool = True, timezone: str = None,
                 origin_db_alias: str = None, target_db_alias: str = None, schema_name: str = None) -> None:
        pipelines.ParallelTask.__init__(self, id=id, description=description,
                                        max_number_of_parallel_tasks=max_number_of_parallel_tasks,
                                        commands_before=commands_before, commands_after=commands_after)

        self.origin_db_alias = origin_db_alias
        self.origin_db = dbs.db(origin_db_alias)
        self.target_db_alias = target_db_alias
        self.target_db = dbs.db(target_db_alias)
        self.schema_name = schema_name
        self.timezone = timezone
        self.echo_queries = echo_queries

    @property
    def db_alias(self):
        return self.origin_db_alias or config.default_db_alias()

    def copy_table_structure(self):
        return f"""(echo 'DROP SCHEMA IF EXISTS {self.schema_name} CASCADE;'; pg_dump --username={self.origin_db.user} --host={self.origin_db.host} --schema={self.schema_name} --section=pre-data --no-owner --no-privileges {self.origin_db.database}) | PGOPTIONS='--client-min-messages=warning' PGTZ=Europe/Berlin psql --username={self.target_db.user} --host={self.target_db.host} --no-psqlrc --set ON_ERROR_STOP=on {self.target_db.database} --quiet""";

    def get_indexes(self):
        get_index_statement = f""" 
        SELECT indexdef AS ddl
        FROM pg_class
        JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        JOIN pg_indexes ON pg_indexes.indexname = pg_class.relname AND pg_indexes.schemaname = nspname
        WHERE nspname = '{self.schema_name}'
        AND relkind = 'i'
        AND pg_indexes.indexname NOT ILIKE '%_chunk%'
        ORDER BY pg_total_relation_size(pg_class.oid) DESC;
        """
        with mara_db.postgresql.postgres_cursor_context(
                self.origin_db_alias) as cursor:  # type: psycopg2.extensions.cursor
            cursor.execute(get_index_statement)
            return ';'.join(str(v[0]) for v in cursor.fetchall()) or []

    def get_functions(self):
        get_function_statement = f"""
        SELECT pg_get_functiondef(pg_proc.oid) AS def 
        FROM (SELECT oid, * FROM pg_proc p WHERE NOT p.proisagg) pg_proc, pg_namespace
        WHERE pg_proc.pronamespace = pg_namespace.oid
        AND nspname = '{self.schema_name}'
        """
        with mara_db.postgresql.postgres_cursor_context(
                self.origin_db_alias) as cursor:  # type: psycopg2.extensions.cursor
            cursor.execute(get_function_statement)
            return ';'.join(str(v[0]) for v in cursor.fetchall()) or 'SELECT 1;'  # select 1 implies do nothing

    def get_all_tables(self):
        get_table_statement = f"""SELECT pg_class.relname AS table,
                      relkind,
                      CASE WHEN relkind = 'f' THEN cstore_table_size(nspname || '.' || relname) 
                      ELSE  pg_total_relation_size(pg_class.oid)
                      END / 1000000.0 as size
                      FROM pg_class
                      JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                      WHERE nspname = '{self.schema_name}'
                      AND relkind in ('r','f')
                      AND relhassubclass = 'f'
                      ORDER BY size DESC"""  # exclude tables with inherited tables
        with mara_db.postgresql.postgres_cursor_context(
                self.origin_db_alias) as cursor:  # type: psycopg2.extensions.cursor
            cursor.execute(get_table_statement)
            return cursor.fetchall() or 'SELECT 1;'  # select 1 implies do nothing

    def add_parallel_tasks(self, sub_pipeline: 'pipelines.Pipeline') -> None:
        # copy table structure
        sub_pipeline.add_initial(pipelines.Task(
            id=f'schema_structure_{self.schema_name}', description=f'Copy schema/table structure',
            commands=[
                bash.RunBash(command=self.copy_table_structure())
            ]))

        ## copy content of tables
        tables = {j: [] for j in range(0, config.max_number_of_parallel_tasks())}
        current_sizes = [0] * config.max_number_of_parallel_tasks()
        for i, table in enumerate(self.get_all_tables()):
            index_min = min(range(len(current_sizes)), key=current_sizes.__getitem__)
            tables[index_min].append(table)
            current_sizes[index_min] += table[2]

        for j in range(0, config.max_number_of_parallel_tasks()):
            if len(tables[j]) > 0:
                not_empty_chunks = j
                id = f'copy_table_chunk_{j}'
                sub_pipeline.add(pipelines.Task(
                    id=id, description=f'Schema copy for {j}',
                    commands=[
                        CopyTable(current_tables=tables[j],
                                  origin_db_alias=self.origin_db_alias, target_db_alias=self.target_db_alias,
                                  schema_name=self.schema_name),
                        CopyCstore(current_tables=tables[j],
                                   origin_db_alias=self.origin_db_alias, target_db_alias=self.target_db_alias,
                                   schema_name=self.schema_name),
                    ]), upstreams=[f'schema_structure_{self.schema_name}'])

        ## copy indices
        indices = {j: str('') for j in range(0, config.max_number_of_parallel_tasks())}
        for i, index in enumerate(self.get_indexes().split(';')):
            indices[i % config.max_number_of_parallel_tasks()] = indices[
                                                                 i % config.max_number_of_parallel_tasks()] + f" {index};"
        for j in range(0, config.max_number_of_parallel_tasks()):
            if len(indices[j]) > 0:
                id = f'copy_index_chunk_{j}'
                sub_pipeline.add(pipelines.Task(
                    id=id, description=f'Index copy chunk {j}',
                    commands=[
                        sql.ExecuteSQL(sql_statement=indices[j], echo_queries=True,
                                       db_alias=self.target_db_alias)
                    ]),
                    upstreams=[f'copy_table_chunk_{chunk}' for chunk in
                               range(0, min(config.max_number_of_parallel_tasks(), not_empty_chunks))])

        sub_pipeline.add_final(pipelines.Task(
            id=f'indexes_{self.schema_name}', description=f'Copies indexes',
            commands=[
                sql.ExecuteSQL(sql_statement=self.get_functions(), echo_queries=True,
                               db_alias=self.target_db_alias)
            ]))

    def html_doc_items(self) -> [(str, str)]:
        return [('db', _.tt[self.origin_db_alias])]


class CopyTable(pipelines.Command):
    def __init__(self, origin_db_alias: str = None, target_db_alias: str = None, schema_name: str = None,
                 current_tables: list = None) -> None:
        super().__init__()
        self.origin_db_alias = origin_db_alias
        self.origin_db = dbs.db(origin_db_alias)
        self.target_db_alias = target_db_alias
        self.target_db = dbs.db(target_db_alias)
        self.schema_name = schema_name
        self.current_tables = current_tables

    def copy_table_command(self, current_table):
        return f"""pg_dump --username={self.origin_db.user} --host={self.origin_db.host} {self.origin_db.database} --section=data -t {self.schema_name}.{current_table} | PGOPTIONS='--client-min-messages=warning' PGTZ=Europe/Berlin psql --username={self.target_db.user} --host={self.target_db.host} --no-psqlrc --set ON_ERROR_STOP=on {self.target_db.database} --quiet; """;

    def command(self):
        command = ''
        for current_table in self.current_tables:
            if current_table[1] == 'r':  # check if normal table
                command += str(self.copy_table_command(current_table[0]))
        return command

    def shell_command(self):
        return self.command()

    def html_doc_items(self) -> [(str, str)]:
        return [
            ('command', html.highlight_syntax(self.shell_command(), 'bash'))
        ]


class CopyCstore(pipelines.Command):
    def __init__(self, origin_db_alias: str = None, target_db_alias: str = None, schema_name: str = None,
                 current_tables: list = None) -> None:
        super().__init__()
        self.origin_db_alias = origin_db_alias
        self.origin_db = dbs.db(origin_db_alias)
        self.target_db_alias = target_db_alias
        self.target_db = dbs.db(target_db_alias)
        self.schema_name = schema_name
        self.current_tables = current_tables

    def copy_cstore_table_command(self, current_table):
        cstore_file_path = {}
        for db_alias in [self.origin_db_alias, self.target_db_alias]:
            get_cstore_location_query = f""" SELECT
            (SELECT setting FROM pg_settings WHERE name = 'data_directory') AS data_dir,
            (SELECT oid FROM pg_database WHERE datname = '{dbs.db(db_alias).database}') AS dboid,
            ftrelid
            FROM pg_foreign_table
            JOIN pg_class ON pg_class.oid = ftrelid
            JOIN pg_namespace ON pg_namespace.oid = relnamespace
            WHERE nspname = '{self.schema_name}' AND relname = '{current_table}';"""
            with mara_db.postgresql.postgres_cursor_context(db_alias) as cursor:  # type: psycopg2.extensions.cursor
                cursor.execute(get_cstore_location_query)
                cstore_location_info = cursor.fetchall()
                if len(cstore_location_info) > 0:
                    cstore_file_path[db_alias] = str(cstore_location_info[0][0]) + '/cstore_fdw/' + str(
                        cstore_location_info[0][1]) + '/' + \
                                                 str(cstore_location_info[0][2])

        if len(cstore_location_info) > 0:
            if dbs.db(self.target_db_alias).host == 'localhost':
                copyCommand = f"""sudo scp {cstore_file_path[self.origin_db_alias]}  {cstore_file_path[self.target_db_alias]}; sudo scp {cstore_file_path[self.origin_db_alias]}.footer {cstore_file_path[self.target_db_alias]}.footer;""";
            else:
                copyCommand = f"""sudo scp {dbs.db(self.origin_db_alias).host}:{cstore_file_path[self.origin_db_alias]}  {dbs.db(self.target_db_alias).host}:{cstore_file_path[self.target_db_alias]}; sudo scp {dbs.db(self.origin_db_alias).host}:{cstore_file_path[self.origin_db_alias]}.footer {dbs.db(self.target_db_alias).host}:{cstore_file_path[self.target_db_alias]}.footer;""";
        else:
            copyCommand = ''
        return copyCommand

    def command(self):
        command = ''
        for current_table in self.current_tables:
            if current_table[1] != 'r':  # check if not normal table
                command += str(self.copy_cstore_table_command(current_table[0]))
        return command

    def shell_command(self):
        return self.command()

    def html_doc_items(self) -> [(str, str)]:
        return [
            ('command', html.highlight_syntax(self.shell_command(), 'bash'))
        ]
