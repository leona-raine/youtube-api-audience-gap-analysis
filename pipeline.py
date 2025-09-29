# pipeline
from data import collect_keyword, save_to_db
from cleanse import clean_yt_data

# change if needed
keywords = ["ai", "tech trends"]
years = [2023, 2024]

db_url = "postgresql+psycopg2://yttrends:YJlQQnIMuBF4c6Xx15Z2@youtube-trends-db.chkegswcel97.ap-southeast-2.rds.amazonaws.com:5432/youtube_trends"


for kw in keywords:
    for yr in years:
        print(f"\nCollecting raw for {kw} ({yr})...")
        df_raw = collect_keyword(kw, yr)
        save_to_db(df_raw, db_url=db_url, table_name="video_stats_raw")
        print(f"Cleaning data for {kw} ({yr})...")
        df_clean = clean_yt_data(df_raw)
        save_to_db(df_clean, db_url=db_url, table_name="video_stats_clean")
        print(f"Done with {kw} ({yr})")