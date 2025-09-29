# 
# light cleansing since light data 
# wrangling is present before extraction
# 
import pandas as pd

def clean_yt_data(df, min_views=50):
    df = df.drop_duplicates(subset="video_id") # 1: drop duplicates
    if min_views > 0:
        df = df[df["views"] >= min_views] # 2: filter out very low-view video counts
    df["likes"] = df["likes"].replace(0, pd.NA) # 3.1: handle missing values or zeroes
    df["comments"] = df["comments"].replace(0, pd.NA) #3.2
    df["engagement_rate"] = (df["likes"].fillna(0) + df["comments"].fillna(0)) / df["views"] # recalculation

    return df