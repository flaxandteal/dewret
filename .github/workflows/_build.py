from portray import config
import yaml

# Note that this does not work for reloads.
mkdocs = config.mkdocs
def _mkdocs(directory: str, **overrides) -> dict:
    superfences = yaml.unsafe_load("""
      preserve_tabs: true
      custom_fences:
        # Mermaid diagrams
        - name: mermaid
          class: mermaid
          format: !!python/name:pymdownx.superfences.fence_code_format
    """)
    print(overrides)
    overrides.setdefault("markdown_extensions", [])
    for n, ext in enumerate(overrides["markdown_extensions"]):
        if ext == "pymdownx.superfences":
            ext = {"pymdownx.superfences": {}}
        elif isinstance(ext, dict) and len(ext) == 1 and "pymdownx.superfences" in ext:
            ...
        else:
            continue
        ext["pymdownx.superfences"].update(superfences)
        overrides["markdown_extensions"][n] = ext
        print(overrides)
    res = mkdocs(directory, **overrides)
    return res
config.mkdocs = _mkdocs

from portray import __main__
