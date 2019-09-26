import collections


class Schema:
    def __init__(self, views=None, tables=None):
        tables = collections.OrderedDict(
            (name, Table(name, **table))
            for name, table in (tables or dict()).items()
        )

        views = collections.OrderedDict(
            (name, View(name, **view))
            for name, view in (views or dict()).items()
        )

        common = set(tables).intersection(views)
        if common:
            raise ValueError("Defined both as table and view: %s" % common)

        tables.update(views)
        self.objects = tables

    def _resolve(self, name, storage):
        try:
            obj = self.objects[name]
        except KeyError:
            obj = Table(name)
            self.objects[name] = obj

        if obj.dependencies:
            for dep in obj.dependencies:
                self._resolve(dep, storage).load(storage)

        return obj

    def query(self, name, storage):
        return self._resolve(name, storage).query(storage)


class View:
    def __init__(self, name, query, dependencies=None, **query_params):
        self.name = name
        self.sql_query = query
        self.dependencies = dependencies
        self.query_params = query_params

    def load(self, storage):
        storage.materialize(self.name, self.sql_query, **self.query_params)

    def query(self, storage):
        return storage.query(self.sql_query, **self.query_params)


class Table:
    def __init__(self, name, **table_params):
        self.name = name
        self.table_params = table_params
        self.dependencies = None

    def load(self, storage):
        storage.load(self.name, **self.table_params)

    def query(self, storage):
        return storage.query(self.name, **self.table_params)
