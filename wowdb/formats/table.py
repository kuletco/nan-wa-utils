import tabulate


class TableFormat:
    def __init__(self, name, **format_params):
        self.name = name
        self.format_params = {'headers': {}}
        self.format_params.update(format_params)

    def render(self, schema, storage, output):
        data = schema.query(self.name, storage)
        text = tabulate.tabulate(data, **self.format_params)
        print(text, file=output)
