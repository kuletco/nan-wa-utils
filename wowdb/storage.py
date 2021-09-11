import contextlib
import logging
import pathlib
import re
import shutil
import sqlite3
import tempfile

import pandas
import requests

import wowdb.exceptions as wdb_exc


VALID_NAME = re.compile(r'^\w+$')
VALID_VERSION = re.compile(r'^\d+(\.\d+){3}$')


class Storage:
    def __init__(self, version, path=None, name=None, object_exists=None, locale=None):
        super().__init__()

        if not VALID_VERSION.match(version):
            raise ValueError("Invalid storage version: %s" % (version))

        if name and not VALID_NAME.match(name):
            raise ValueError("Invalid storage name: %s" % (name))

        self.url = "https://wow.tools/api/export/"
        self.version = version
        self.storage_path = path and pathlib.Path(path)
        self.context = contextlib.ExitStack()
        self.db_connection = None
        self.storage_name = name
        self.objects = dict()
        self.locale = locale

        try:
            object_exists = object_exists or 'warn'
            self.on_object_exists = getattr(
                self, "_on_object_exists_%s" % object_exists)
        except AttributeError:
            raise ValueError(
                "Invalid object_exists action: %s" % object_exists
            ) from None

    def __str__(self):
        return "WoWDB[%s @ %s; version %s]" % (
            self.storage_name or 'MEMORY',
            self.storage_path or 'TMPDIR',
            self.version)

    def __enter__(self):
        logging.info("Opening storage %s", self)

        if self.db_connection:
            raise wdb_exc.DBAccessError(
                "Attempt to re-open storage %s" % self)

        if not self.storage_path:
            tmpdir_ctx = tempfile.TemporaryDirectory()
            tmpdir = self.context.enter_context(tmpdir_ctx)
            self.storage_path = pathlib.Path(tmpdir)
        else:
            self.storage_path = self.storage_path / self.version
            self.storage_path.mkdir(exist_ok=True)

        dbfile = ":memory:"
        if self.storage_name:
            dbfile = self.storage_path / (self.storage_name + ".sqlite")

        db_ctx = contextlib.closing(sqlite3.connect(dbfile))
        self.db_connection = self.context.enter_context(db_ctx)

        logging.info("Opened storage %s", self)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.close()

    def _table_path(self, table):
        if not self.db_connection:
            raise wdb_exc.DBAccessError("Database not open: %s" % self)
        if not VALID_NAME.match(table):
            raise wdb_exc.DBTableError(
                "Invalid table name: %s" % table)
        return self.storage_path / ("%s.csv" % table)

    def _download_table(self, table):
        params = {"name": table, "build": self.version, "locale": self.locale}
        logging.info("Downloading table [%s]", table)
        file_path = self._table_path(table)

        with file_path.open('wb') as output:
            with requests.get(self.url, params=params, stream=True) as rq:
                rq.raise_for_status()
                shutil.copyfileobj(rq.raw, output)

        return file_path

    def _open_table(self, table):
        try:
            return self._table_path(table).open('rt', encoding='utf-8')
        except FileNotFoundError:
            try:
                return self._download_table(table).open('rt', encoding='utf-8')
            except requests.HTTPError as exc:
                if exc.response.status_code == requests.codes.not_found:
                    raise wdb_exc.DBTableError(
                        "Table [%s] not found" % table) from exc
                raise exc from None

    def load(self, table, **table_params):
        is_new = False
        with self._open_table(table) as f:
            try:
                if self.objects[table] != f.name:
                    # TODO: proper exception
                    raise wdb_exc.DBTableError("EEXIST")
                self.on_object_exists(table)
            except KeyError:
                df = pandas.read_csv(f, **table_params)
                df.to_sql(table, self.db_connection)
                self.objects[table] = f.name
                is_new = True
        return is_new

    def query(self, sql, **query_params):
        if VALID_NAME.match(sql):
            self.load(sql, **query_params)
            sql = "select * from %s" % (sql)
        df = pandas.read_sql(sql, self.db_connection, **query_params)
        return df.to_dict(orient='records')

    def materialize(self, view, sql, **query_params):
        is_new = False
        try:
            if sql != self.objects[view]:
                # TODO: proper exception
                raise wdb_exc.DBTableError("EEXIST")
            self.on_object_exists(view)
        except KeyError:
            df = pandas.read_sql(sql, self.db_connection, **query_params)
            df.to_sql(view, self.db_connection)
            self.objects[view] = sql
            is_new = True
        return is_new

    def _on_object_exists_skip(self, name):
        pass

    def _on_object_exists_warn(self, name):
        logging.warning("Object already loaded: %s", name)
