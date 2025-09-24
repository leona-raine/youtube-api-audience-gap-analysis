# INSTRUCTION:
# 
# option 1: run python parse.py (name of _stats.csv)
# option 2: run python parse.py (name of _stats.csv) --min_views (value)
# 
# for extracting cleansed data
import argparse
from cleanse import clean_yt_data
import os
import sys

raw_dir = os.path.join("storage", "raw")
processed_dir = os.path.join("storage", "processed")

if __name__ =="__main__":
    # parsing _stats.py file
    if len(sys.argv) < 2:
        print("Usage: python parse.py <filename>")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Clean YouTube data")
    parser.add_argument("input_file", type=str, help="Name of input CSV (must be in storage/)")
    parser.add_argument("--min_views", type=int, default=0, help="Minimum views filter (default: 0 = 0 no filter)")
    args = parser.parse_args()

    input_path = os.path.join(raw_dir, args.input_file)
    output_name = args.input_file.replace("_stats", "_pp")
    output_path = os.path.join(processed_dir, output_name)

    df_cleaned = clean_yt_data(input_path, min_views=args.min_views)

    os.makedirs(processed_dir, exist_ok=True)

    # overwrite settings
    if os.path.exists(output_path):
        confirm = input(f"File '{output_name}' already exists in processed/. Replace it? (y/n): ").strip().lower()
        if confirm !="y":
            print("Save cancelled.")
            sys.exit(0)
    else:
        print(f"Deleted old file: {output_name}")
        os.remove(output_path)

    df_cleaned.to_csv(output_path, index=False)
    print(f"Cleaned file saved as {output_path}")
    print(df_cleaned.describe())