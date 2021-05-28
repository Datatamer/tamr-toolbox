"""Generate a PDF of the Tamr Toolbox doc_src
Requires installation of wkhtmltopdf, Mac install command: brew cask install wkhtmltopdf
"""
from pathlib import Path
import pdfkit


def main():
    """Generates a pdf based on the html files located in docs/_draft_build"""

    build_path = Path(__file__).parent.parent.resolve() / "docs" / "_draft_build"

    # Generate a list of file paths in the order they should be rendered in the PDF
    all_files = []

    # introduction files
    all_files.extend(
        [
            build_path / name
            for name in ["index.html", "installation.html", "examples.html", "modules.html"]
        ]
    )
    # example files
    all_files.extend(sorted(list(Path(build_path / "examples").rglob("*.html"))))
    # module files
    all_files.extend(sorted(list(Path(build_path / "modules").rglob("*.html"))))
    # keyword index file
    all_files.append(build_path / "genindex.html")

    # Generate the pdf file
    output_file_path = build_path / "tamr-toolbox.pdf"
    options = {"allow": ".", "javascript-delay": 6000}

    pdfkit.from_file(all_files, output_file_path, options=options)


if __name__ == "__main__":
    main()
