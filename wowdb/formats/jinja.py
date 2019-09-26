import jinja2


class TemplateFormat:
    def __init__(self, name, template, dependencies=None):
        self.name = name
        self.dependencies = dependencies or (name,)
        self.env = jinja2.Environment(
            loader=jinja2.DictLoader({
                name: template
            })
        )

    def render(self, schema, storage, output):
        data = {
            dep: schema.query(dep, storage)
            for dep in self.dependencies
        }
        template = self.env.get_template(self.name)
        template.stream(**data).dump(output)
        print("", file=output)
