"""
Download the PaySim dataset, strip the fraud-flag columns, write a clean
copy to the working directory, and delete the cached download.
"""
 
import shutil
from pathlib import Path
 
import kagglehub
import pandas as pd
 
 
def main():
    dataset_dir = Path(kagglehub.dataset_download("mtalaltariq/paysim-data"))
    print("paysim data downloaded to: ", dataset_dir)
 
    # find the CSV inside the downloaded directory
    csv_files = list(dataset_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV found in {dataset_dir}")
    source_csv = csv_files[0]
 
    df = pd.read_csv(source_csv)
    df = df.drop(columns=["isFraud", "isFlaggedFraud"])
 
    output_path = Path.cwd() / "paysim_clean.csv"
    df.to_csv(output_path, index=False)
 
    # walk up to the dataset's root folder in the cache (the one named after
    # the dataset, sitting under .../datasets/<owner>/) and remove it entirely
    dataset_root = dataset_dir
    while dataset_root.parent.name != "datasets" and dataset_root.parent != dataset_root:
        dataset_root = dataset_root.parent
    shutil.rmtree(dataset_root, ignore_errors=True)
 
    print(f"Clean dataset written to {output_path}")
    print(f"Cached dataset removed from {dataset_root}")
 
 
if __name__ == "__main__":
    main()