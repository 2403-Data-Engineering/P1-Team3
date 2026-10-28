"""Write bounded unlabeled JSONL events; never download data or renumber a sample."""
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from itertools import islice
from pathlib import Path

from contracts.transaction_event import PAYSIM_FIELD_ORDER, build_transaction_event

DEFAULT_SAMPLE_SIZE = 100_000


def sample_csv(source: Path, destination: Path, *, ingested_at: datetime,
               limit: int = DEFAULT_SAMPLE_SIZE, source_file: str | None = None) -> int:
    if not 1 <= limit <= DEFAULT_SAMPLE_SIZE:
        raise ValueError("sample size must be between 1 and 100000")
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation: no silent overwrite of a previous sample or source.
    with source.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or set(reader.fieldnames) != set(PAYSIM_FIELD_ORDER):
            raise ValueError("expected only the unlabeled operational CSV columns")
        try:
            with destination.open("x", encoding="utf-8", newline="\n") as output:
                count = 0
                for row_number, row in enumerate(islice(reader, limit), 1):
                    event = build_transaction_event(row, source_file=source_file or source.name,
                                                    row_number=row_number, ingested_at=ingested_at)
                    output.write(json.dumps(event, sort_keys=True, allow_nan=False) + "\n")
                    count += 1
                return count
        except FileExistsError:
            raise
        except Exception:
            destination.unlink(missing_ok=True)  # Remove only this call's incomplete output.
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--limit", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument("--source-file", help="Original ops basename, shared with eval/ingest")
    parser.add_argument("--ingested-at", required=True, help="Fixed ISO timestamp for reproducible output")
    args = parser.parse_args()
    print("sample_rows=", sample_csv(args.source, args.destination, limit=args.limit,
          source_file=args.source_file, ingested_at=datetime.fromisoformat(args.ingested_at.replace("Z", "+00:00"))))


if __name__ == "__main__":
    main()
