"""Build the Glue dependency ZIP locally; deliberately performs no upload."""
from __future__ import annotations

import argparse
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED


def build_package(destination: Path):
    root = Path(__file__).resolve().parent.parent
    destination.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(destination, "x", compression=ZIP_DEFLATED) as archive:
        for package in ("contracts", "pipeline"):
            for path in sorted((root / package).iterdir()):
                if path.suffix in {".py", ".json"}:
                    archive.write(path, path.relative_to(root).as_posix())
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    build_package(parser.parse_args().output)
