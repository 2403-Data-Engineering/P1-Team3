"""
Download the PaySim dataset, write an unlabeled operational CSV, and write a
labeled evaluation sidecar keyed by txn_id. The Kaggle cache is then deleted.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from contracts.transaction_event import txn_id_for_csv_row

LABEL_COLUMNS = ("isFraud", "isFlaggedFraud")
OPS_CSV_NAME = "paysim_clean.csv"
EVAL_DIR_NAME = "eval"
EVAL_CSV_NAME = "paysim_eval_labels.csv"
EVAL_COLUMNS = ("txn_id", "source_file", "row_number", "isFraud", "isFlaggedFraud")


def _as_flag(value) -> int:
    if pd.isna(value):
        raise ValueError("evaluation labels cannot be null")
    flag = int(value)
    if flag not in (0, 1):
        raise ValueError(f"label must be 0 or 1, got {value!r}")
    return flag


def split_ops_and_eval(
    labeled: pd.DataFrame, *, ops_source_file: str = OPS_CSV_NAME
) -> tuple[pd.DataFrame, pd.DataFrame]:
    missing = [column for column in LABEL_COLUMNS if column not in labeled.columns]
    if missing:
        raise ValueError(f"labeled PaySim input is missing columns: {missing}")

    ops = labeled.drop(columns=list(LABEL_COLUMNS)).copy()
    if any(column in ops.columns for column in LABEL_COLUMNS):
        raise ValueError("operational frame must not contain fraud labels")

    eval_rows: list[dict[str, object]] = []
    for row_number, (ops_record, labeled_record) in enumerate(
        zip(ops.to_dict("records"), labeled.to_dict("records"), strict=True),
        start=1,
    ):
        txn_id = txn_id_for_csv_row(
            ops_record,
            source_file=ops_source_file,
            row_number=row_number,
        )
        eval_rows.append(
            {
                "txn_id": txn_id,
                "source_file": ops_source_file,
                "row_number": row_number,
                "isFraud": _as_flag(labeled_record["isFraud"]),
                "isFlaggedFraud": _as_flag(labeled_record["isFlaggedFraud"]),
            }
        )

    eval_df = pd.DataFrame(eval_rows, columns=list(EVAL_COLUMNS))
    if eval_df["txn_id"].duplicated().any():
        raise ValueError("eval sidecar txn_id values must be unique")
    if len(eval_df) != len(ops):
        raise ValueError("eval sidecar must be 1:1 with operational rows")
    return ops, eval_df


def write_ops_and_eval(
    labeled: pd.DataFrame,
    *,
    ops_path: Path,
    eval_path: Path,
    ops_source_file: str | None = None,
) -> tuple[Path, Path]:
    source_file = ops_source_file or ops_path.name
    ops, eval_df = split_ops_and_eval(labeled, ops_source_file=source_file)
    ops_path.parent.mkdir(parents=True, exist_ok=True)
    eval_path.parent.mkdir(parents=True, exist_ok=True)
    ops.to_csv(ops_path, index=False)
    eval_df.to_csv(eval_path, index=False)
    return ops_path, eval_path


def _kaggle_dataset_root(dataset_dir: Path) -> Path:
    dataset_root = dataset_dir
    while dataset_root.parent.name != "datasets" and dataset_root.parent != dataset_root:
        dataset_root = dataset_root.parent
    return dataset_root


def main() -> None:
    import kagglehub

    dataset_dir = Path(kagglehub.dataset_download("mtalaltariq/paysim-data"))
    print("paysim data downloaded to: ", dataset_dir)

    csv_files = list(dataset_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV found in {dataset_dir}")
    source_csv = csv_files[0]

    labeled = pd.read_csv(source_csv)
    ops_path = Path.cwd() / OPS_CSV_NAME
    eval_path = Path.cwd() / EVAL_DIR_NAME / EVAL_CSV_NAME
    write_ops_and_eval(labeled, ops_path=ops_path, eval_path=eval_path)

    dataset_root = _kaggle_dataset_root(dataset_dir)
    shutil.rmtree(dataset_root, ignore_errors=True)

    print(f"Operational dataset written to {ops_path}")
    print(f"Evaluation labels written to {eval_path}")
    print(f"Cached dataset removed from {dataset_root}")


if __name__ == "__main__":
    main()
