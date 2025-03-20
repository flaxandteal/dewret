"""Utility file used to generate recipe.yaml."""

import textwrap
import toml

with open("pyproject.toml") as f:
    pyproject = toml.load(f)

with open("README.md") as f:
    readme_string = f.read()

readme_string = textwrap.indent(readme_string, "    ")

with open("recipe.yaml") as f:
    TEMPLATE = f.read()

deps = pyproject["project"]["dependencies"]
deps_string = ""
for dep in deps:
    deps_string += f"    - {dep}\n"
TEMPLATE = TEMPLATE.replace("__PROJECT_DEPENDENCIES__", deps_string)
TEMPLATE = TEMPLATE.replace("__README__", readme_string)
with open("recipe.yaml", "w") as f:
    f.write(TEMPLATE)
