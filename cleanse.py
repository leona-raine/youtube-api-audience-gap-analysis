# 
# light cleansing since light data 
# wrangling is present before extraction
# 
import pandas as pd

def clean_yt_data(input_file, min_views=50):
    df = pd.read_csv(input_file)
    df = df.drop_duplicates(subset="video_id") # 1: drop duplicates
    df = df[df["views"] >= min_views] # 2: filter out very low-view video counts
    df["likes"] = df["likes"].replace(0, pd.NA) # 3.1: handle missing values or zeroes
    df["comments"] = df["comments"].replace(0, pd.NA) #3.2
    df["engagement_rate"] = (df["likes"].fillna(0) + df["comments"].fillna(0)) / df["views"] # recalculation

    output_file = input_file.replace(".csv", "_pp.csv")
    df.to_csv(output_file, index=False)

    print(f"Cleaned file saved as {output_file}")
    return df

if __name__ == "__main__":
    df_cleaned = clean_yt_data("yt_stats.csv", min_views=100)
    print(df_cleaned.describe())