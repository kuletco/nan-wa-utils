import wowdb.formats.jinja as fmt_jinja
import wowdb.formats.table as fmt_table


FORMATS = {
    'table': fmt_table.TableFormat,
    'jinja': fmt_jinja.TemplateFormat,
}


class Output:
    def __init__(self, output_config):
        self.default_format = output_config.pop('default', {
            'kind': 'table',
            'tablefmt': 'presto',
        })
        self.renderers = {
            name: self._create(name, **format_params)
            for name, format_params in output_config.items()
        }

    def _create(self, name, kind, **format_params):
        return FORMATS[kind](name, **format_params)

    def _get(self, name):
        try:
            renderer = self.renderers[name]
        except KeyError:
            renderer = self._create(name, **self.default_format)
            self.renderers[name] = renderer
        return renderer

    def render(self, name, schema, storage, output):
        self._get(name).render(schema, storage, output)
