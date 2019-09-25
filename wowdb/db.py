import contextlib
import csv
import logging
import pandas
import pathlib
import re
import requests
import shutil
import sqlite3
import tempfile

import wowdb.exceptions as db_exc


class DB:
    def __init__(self, build, path=None, dbname=None):
        self.url = "https://wow.tools/api/export/"
        self.build = build
        self.path = path and pathlib.Path(path)
        self.name_regex = re.compile(r'^\w+$')
        self.context = contextlib.ExitStack()
        self.db = None
        self.dbname = dbname

        if dbname and not self.name_regex.match(dbname):
            raise ValueError("Invalid database name: %s" % (dbname))
    
    def __str__(self):
        return "WoWDB[%s @ %s; build %s]" % (
            self.dbname or 'MEMORY',
            self.path or 'TMPDIR',
            self.build)

    def __enter__(self):
        logging.info("Opening database %s" % self)

        if self.db:
            raise db_exc.DBAccessError(
                "Attempt to re-open database %s" % self)

        if not self.path:
            tmpdir_ctx = tempfile.TemporaryDirectory()
            tmpdir = self.context.enter_context(tmpdir_ctx)
            self.path = pathlib.Path(tmpdir)
        else:
            self.path = self.path / self.build
            self.path.mkdir(exist_ok=True)
        
        dbfile = ":memory:"
        if self.dbname:
            dbfile = self.path / (self.dbname + ".sqlite")
        
        db_ctx = contextlib.closing(sqlite3.connect(dbfile))
        self.db = self.context.enter_context(db_ctx)

        logging.info("Database open: %s", self)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.close()

    def _table_path(self, table):
        if not self.db:
            raise db_exc.DBAccessError("Database not open: %s" % self)
        if not self.name_regex.match(table):
            raise db_exc.DBTableError(
                "Invalid table name: %s" % table)
        return self.path / ("%s.csv" % table)

    def _download_table(self, table):
        params = {"name": table, "build": self.build}
        logging.info("Downloading table [%s]", table)
        file_path = self._table_path(table)

        with file_path.open('wb') as output:
            with requests.get(self.url, params=params, stream=True) as rq:
                shutil.copyfileobj(rq.raw, output)

        return file_path
    
    def _open_table(self, table):
        try:
            return self._table_path(table).open('rt', encoding='utf-8')
        except FileNotFoundError:
            return self._download_table(table).open('rt', encoding='utf-8')

    def load(self, table, **table_params):
        with self._open_table(table) as f:
            df = pandas.read_csv(f, **table_params)
            df.to_sql(table, self.db)
    
    def query(self, sql, **query_params):
        df = pandas.read_sql(sql, self.db, **query_params)
        return df.to_dict(orient='index')
