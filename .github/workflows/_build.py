from portray import config, render  # type: ignore
from typing import Any
import yaml

# Note that this does not work for reloads.
mkdocs = config.mkdocs


def _mkdocs(directory: str, **overrides) -> dict:  # type: ignore
    superfences = yaml.unsafe_load("""
      preserve_tabs: true
      custom_fences:
        # Mermaid diagrams
        - name: mermaid
          class: mermaid
          format: !!python/name:pymdownx.superfences.fence_code_format
    """)
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
    res: dict[Any, Any] = mkdocs(directory, **overrides)
    return res


config.mkdocs = _mkdocs

mkdocs_render = render.mkdocs


def _mkdocs_render(config: dict[str, Any]) -> Any:
    """Ensures the original config can be reused."""
    original_config = list(config.get("markdown_extensions", []))
    if original_config and "pymdownx.superfences" in original_config[0]:
        original_config = original_config[0]["pymdownx.superfences"]
        result = mkdocs_render(config)
        config["markdown_extensions"][0]["pymdownx.superfences"] = original_config
    else:
        result = mkdocs_render(config)
    return result


render.mkdocs = _mkdocs_render

if __name__ == "__main__":
    import sys
    import subprocess
    from pathlib import Path
    from portray.cli import cli  # type: ignore

    # Run the standard portray build
    cli()

    # If we're building for GitHub Pages, also generate the presentation PDF
    if len(sys.argv) > 1 and "on-github-pages" in sys.argv:
        script_dir = Path(__file__).parent
        pdf_script = script_dir / "generate_presentation_pdf.py"
        output_pdf = Path("site") / "presentation.pdf"

        print("Generating presentation PDF...")

        # Install playwright browsers if needed
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"], check=True
        )

        # Generate the PDF
        subprocess.run([sys.executable, str(pdf_script), str(output_pdf)], check=True)

        print(f"Presentation PDF generated at: {output_pdf}")
