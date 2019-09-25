import argparse
import logging
import yaml

import wowdb.script as script


class CLI:
    def __init__(self):
        self.args = self._get_args()
        self.script = self._load_script()

    def _get_args(self):
        parser = argparse.ArgumentParser(
            description='Query WoW DB'
        )
        parser.add_argument(
            "script",
            help="path to the script to execute"
        )
        return parser.parse_args()

    def _load_script(self):
        try:
            from yaml import CLoader as Loader
        except ImportError:
            from yaml import Loader
        with open(self.args.script) as f:
            return script.Script(yaml.load(f, Loader=Loader))

    def run(self):
        self.script.execute()


def run():
    cli = CLI()
    cli.run()

    
if __name__ == "__main__":
    run()
