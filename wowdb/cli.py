import argparse
import logging
import sys

import yaml

try:
    from yaml import CLoader as YAMLLoader
except ImportError:
    from yaml import Loader as YAMLLoader

import wowdb.output as wdb_out
import wowdb.schema as wdb_schema
import wowdb.storage as wdb_storage


class CLI:
    def __init__(self):
        self.args = self._get_args()
        self.config = self._load_config()

    def _get_args(self):
        parser = argparse.ArgumentParser(
            description='Query WoW Tools Database'
        )
        parser.add_argument(
            "-d", "--debug",
            help="enable debug output",
            action='store_const',
            const=logging.DEBUG, default=logging.WARNING,
            dest='log_level'
        )
        parser.add_argument(
            "-o", "--output", type=str, metavar="FILE",
            help="output to a file",
            dest='output_file', default='-'
        )
        parser.add_argument(
            "-c", "--config", type=str, metavar="FILE",
            help="configuration file location",
            dest='config_file',
            required=True
        )
        parser.add_argument(
            "object_name", metavar="OBJECT",
            help="name of the object to query"
        )
        return parser.parse_args()

    def _load_config(self):
        with open(self.args.config_file) as f:
            return yaml.load(f, Loader=YAMLLoader)

    def _setup_logging(self):
        fmt = ("%(asctime)s %(levelname)s "
               "[%(name)s] %(message)s")
        logging.basicConfig(level=self.args.log_level, format=fmt)

    def _get_storage(self):
        return wdb_storage.Storage(**self.config['storage'])

    def _get_schema(self):
        return wdb_schema.Schema(**self.config.get('schema', {}))

    def _get_output(self):
        return wdb_out.Output(self.config.get('output', {}))

    def run(self):
        self._setup_logging()

        with self._get_storage() as storage:
            schema = self._get_schema()
            output = self._get_output()
            name = self.args.object_name
            out_file = self.args.output_file
            if out_file == '-':
                output.render(name, schema, storage, sys.stdout)
            else:
                with open(out_file, 'wt') as f:
                    output.render(name, schema, storage, f)


def run():
    cli = CLI()
    cli.run()


if __name__ == "__main__":
    run()
