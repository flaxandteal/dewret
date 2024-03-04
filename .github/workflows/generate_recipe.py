from jinja2 import Environment, FileSystemLoader
import toml

with open("pyproject.toml") as f:
    pyproject = toml.load(f)

with open(".github/workflows/recipe.yaml.j2") as f:
    TEMPLATE = f.read()

template = Environment(loader=FileSystemLoader("./")).from_string(TEMPLATE)
print(template.render(pyproject=pyproject))
