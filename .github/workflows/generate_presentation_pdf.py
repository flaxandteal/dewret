#!/usr/bin/env python3
"""Generate PDF from the Reveal.js presentation."""

import asyncio
import os
import sys
from pathlib import Path
from playwright.async_api import async_playwright
import http.server
import socketserver
import threading
import time


def start_server(directory: Path, port: int = 8000) -> socketserver.TCPServer:
    """Start a simple HTTP server to serve the presentation."""
    os.chdir(directory)
    handler = http.server.SimpleHTTPRequestHandler
    httpd = socketserver.TCPServer(("", port), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    time.sleep(2)  # Give the server time to start
    return httpd


async def generate_pdf(presentation_dir: Path, output_pdf: Path) -> None:
    """Generate PDF from Reveal.js presentation using Playwright."""
    # Start HTTP server
    port = 8000
    server = start_server(presentation_dir, port)

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()

            # Navigate to the presentation with print-pdf parameter
            url = f"http://localhost:{port}/?print-pdf"
            await page.goto(url, wait_until="networkidle")

            # Wait for Reveal.js to initialize and render
            await page.wait_for_function(
                "() => window.Reveal && window.Reveal.isReady()"
            )
            await page.wait_for_timeout(2000)  # Extra time for rendering

            # Generate PDF with appropriate settings
            await page.pdf(
                path=str(output_pdf),
                format="A4",
                landscape=True,
                print_background=True,
                margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
            )

            await browser.close()
            print(f"PDF generated: {output_pdf}")
    finally:
        server.shutdown()


async def main():
    """Main function to generate presentation PDF."""
    # Determine paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    presentation_dir = project_root / "resources" / "presentation"

    # Output path - can be customized via command line argument
    if len(sys.argv) > 1:
        output_pdf = Path(sys.argv[1])
    else:
        output_pdf = project_root / "docs" / "presentation.pdf"

    # Ensure output directory exists
    output_pdf.parent.mkdir(parents=True, exist_ok=True)

    # Generate PDF
    await generate_pdf(presentation_dir, output_pdf)


if __name__ == "__main__":
    asyncio.run(main())
