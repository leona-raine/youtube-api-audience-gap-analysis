# INSTRUCTION:
# 
# Input keyword and the appropriate year (2007 and up)
#
#  ----------------------------------------------------------------
#
# for data collection and extraction
from googleapiclient.discovery import build
import pandas as pd
import os
import datetime
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("YOUTUBE_API_KEY")

def get_youtube_client():
    return build("youtube", "v3", developerKey=api_key)

youtube = get_youtube_client()

# fetch api multiple yt vid stats simultaneously
def get_vid_stats_batch(video_ids):
    req = youtube.videos().list(
        part="statistics",
        id=",".join(video_ids)
    )
    response = req.execute()
    data = []
    for item in response["items"]:
        stats = item["statistics"]
        data.append({
            "video_id": item["id"],
            "views": int(stats.get("viewCount", 0)),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0))
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
    if year < 2005:
        return False
    if year > current_year:
        return False
    return True

# data stats collection + engagement rate
def collect_keyword(keyword, start_year, max_results=100):
    print(f"Collecting numeric stats for: {keyword}'...")
    df = search_videos(keyword, start_year=start_year, max_results=max_results)
    df["keyword"] = keyword
    # feature engineering for quick inspection
    df["engagement_rate"] = (df["likes"] + df["comments"]) / df["views"].replace(0, 1) 
    return df

# saving csv and overwriting existing .csv file
def save_to_csv(df, keyword, start_year):
    folder = os.path.join("storage", "raw")
    os.makedirs(folder, exist_ok=True)

    filename = f"{keyword}_{start_year}_stats.csv"
    filepath = os.path.join(folder, filename)

    # Only check THIS file
    if os.path.exists(filepath):
        confirm = input(f"File '{filename}' already exists in {folder}. Replace it? (y/n): ").strip().lower()
        if confirm != "y":
            print("Save cancelled.")
            return
        else:
            os.remove(filepath)
            print(f"Deleted old file: {filename}")

    # Always save in storage/raw
    df.to_csv(filepath, index=False)
    print(f"✅ Saved new file as {filepath}")

# Main execution on py file
if __name__ == "__main__":
    # User input
    while True:
        keyword = input("Enter niche keyword (or press Enter if you wish to exit or overwrite your previous keyword): ").strip()
        if keyword == "":
            print("Existing program.")
            break
        start_year_str = input("Enter start year (YYYY): ")
        if not start_year_str.isdigit():
            print("Please enter a valid year (numbers only).")
            continue
        start_year = int(start_year_str)
        if not validate_year(start_year):
            print(f"Invalid year: {start_year}. Must be between 2005 and {datetime.datetime.now().year}.")
            continue

        try:
            df = collect_keyword(keyword, start_year=start_year, max_results=100) # change max results if needed (max. 100)
            print(df.describe()) # change peek-data info if needed
            save_to_csv(df, keyword, start_year) # save csv/overwrite csv
        except ValueError as e:
            print(f"Error: {e}")