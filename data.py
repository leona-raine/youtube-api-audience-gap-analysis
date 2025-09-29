# for data collection and extraction
from googleapiclient.discovery import build
import pandas as pd
import os
import datetime
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
api_key = os.getenv("YOUTUBE_API_KEY")

def get_youtube_client():
    return build("youtube", "v3", developerKey=api_key)

youtube = get_youtube_client()

# fetch api multiple yt vid stats simultaneously
def get_vid_stats_batch(video_ids):
    req = youtube.videos().list(
        part="snippet,statistics",
        id=",".join(video_ids)
    )
    response = req.execute()
    data = []
    for item in response["items"]:
        stats = item["statistics"]
        snippet = item["snippet"]
        data.append({
            "video_id": item["id"],
            "views": int(stats.get("viewCount", 0)),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0)),
            "published_at": snippet.get("publishedAt"),
            "collected_at": datetime.datetime.now()
        })
    return data

# keyword search yt vid
def search_videos(keyword, start_year, max_results=100):
    video_data = []
    next_page_token = None
    collected = 0
    published_after = f"{start_year}-01-01T00:00:00Z"
    published_before = f"{start_year+1}-01-01T00:00:00Z"

    while collected < max_results:
        request = youtube.search().list(
            part="id",
            q=keyword,
            type="video",
            maxResults=min(50, max_results - collected),
            pageToken=next_page_token,
            publishedAfter=published_after,
            publishedBefore=published_before
        )
        response = request.execute()

        # date error-handling
        if not response.get("items"):
            raise ValueError(f"No videos found for '{keyword} in {start_year}.")

        video_ids = [item["id"]["videoId"] for item in response["items"]] 
        batch_data = get_vid_stats_batch(video_ids)
        video_data.extend(batch_data)

        collected += len(video_ids)
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return pd.DataFrame(video_data)

# date year correction
def validate_year(year: int) -> bool:
    current_year = datetime.datetime.now().year
    return 2005 <= year <= current_year

# data stats collection + engagement rate
def collect_keyword(keyword, start_year, max_results=100):
    print(f"Collecting numeric stats for: {keyword} ({start_year})'...")
    df = search_videos(keyword, start_year=start_year, max_results=max_results)
    df["keyword"] = keyword
    # feature engineering for quick inspection
    df["engagement_rate"] = (df["likes"] + df["comments"]) / df["views"].replace(0, 1) 
    return df

# saving data to db
def save_to_db(df, db_url, table_name="video_stats"):
    """
    Save DataFrame to PostgreSQL database.
    """
    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Saved {len(df)} rows to table '{table_name}'")