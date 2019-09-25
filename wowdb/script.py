import collections
import copy
import jinja2
import sys

import wowdb.db as db

class Script:
    class Node:
        pass

    def __init__(self, config):
        config = copy.deepcopy(config)
        self.db = self._load_db_config(config)
        self.queries = collections.OrderedDict(
            (name, self._load_query_config(query_config))
            for name, query_config in config['queries'].items()
        )
        self.output = collections.OrderedDict(
            (name, self._load_output_config(output_config))
            for name, output_config in config['output'].items()
        )

    def _load_db_config(self, config):
        db = Script.Node()
        db_cfg = config['db']

        db.build = db_cfg.get('build')
        db.path = db_cfg.get('path', None)
        db.name = db_cfg.get('name', None)
        db.tables = [
            self._load_table_config(table_cfg)
            for table_cfg in db_cfg.get('tables', [])]

        return db

    def _load_table_config(self, table_config):
        table = Script.Node()

        try:
            table.name = table_config.pop('table')
            table.params = table_config
        except AttributeError:
            table.name = table_config
            table.params = {}

        return table
    
    def _load_query_config(self, query_config):
        query = Script.Node()

        query.sql = query_config.pop('sql')
        query.params = query_config

        return query
    
    def _load_output_config(self, output_config):
        output = Script.Node()

        output.format = output_config.pop('format')
        output.file = output_config.pop('file', sys.stdout)

        if output.format == 'jinja2':
            output.template = output_config.pop('template', None)
        else:
            raise ValueError("Unsupported output format: " % output.format)
    
        return output
    
    def _open_db(self):
        return db.DB(
            self.db.build,
            self.db.path,
            self.db.name)
    
    def execute(self):
        template_env = jinja2.Environment(
            loader=jinja2.DictLoader({
                name: output.template
                for name, output in self.output.items()
            })
        )

        with self._open_db() as db:
            for table in self.db.tables:
                db.load(table.name, **table.params)

            results = {
                name: db.query(query.sql, **query.params)
                for name, query in self.queries.items()
            }

            for name, output in self.output.items():
                template = template_env.get_template(name)
                template.stream(**results).dump(output.file)
