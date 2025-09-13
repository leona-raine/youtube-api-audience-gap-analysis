# for data collection and extraction
from googleapiclient.discovery import build
import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("YOUTUBE_API_KEY")

def get_youtube_client():
    return build("youtube", "v3", developerKey=api_key)

youtube = get_youtube_client()

# full stats for a yt vid
def get_vid_stats(video_id):
    stats_req = youtube.videos().list(
        part="statistics,contentDetails,snippet", id=video_id
    )
    stats_item = stats_req.execute()["items"][0]

    statistics = stats_item["statistics"]
    snippet = stats_item["snippet"]
    content_details = stats_item["contentDetails"]

    return {
        "video_id": video_id,
        "title": snippet.get("title"),
        "description": snippet.get("description"),
        "channel_id": snippet.get("channelId"),
        "channel_title": snippet.get("channelTitle"),
        "category_id": snippet.get("categoryId"),
        "views": int(statistics.get("viewCount", 0)),
        "likes": int(statistics.get("likeCount", 0)),
        "comments": int(statistics.get("commentCount", 0)),
        "duration": content_details.get("duration"),
        "published_at": snippet.get("publishedAt")
    }

# search vidoes by keyword with date filter 2020
def search_videos(query, max_results=100, start_year=2020):
    video_data = []
    next_page_token = None
    collected = 0
    published_after = f"{start_year}-01-01T00:00:00Z"

    while collected < max_results:
        request = youtube.search().list(
            part="snippet",
            q=query,
            type="video",
            maxResults=min(50, max_results - collected),
            pageToken=next_page_token,
            publishedAfter=published_after
        )
        response = request.execute()

        for item in response["items"]:
            video_id = item["id"]["videoId"]
            data = get_vid_stats(video_id)
            video_data.append(data)
            collected += 1
            if collected >= max_results:
                break

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

        time.sleep(1) # quota issues to avoid

    return pd.DataFrame(video_data)

# multiple keywords used simultaneously
def collect_keywords(keywords, max_results_per_keyword=100, start_year=2020):
    all_data = pd.DataFrame()

    for keyword in keywords:
        print(f"Collecting videos for keyword: {keyword}")
        df = search_videos(keyword, max_results=max_results_per_keyword, start_year=start_year)
        df["keyword"] = keyword
        all_data = pd.concat([all_data, df], ignore_index=True)

    return all_data

def csv_save(df, filename="ytraw.csv"):
    df.to_csv(filename, index=False)
    
# collecting raw data (testing/collection)

keywords = ["skyrim","rimworld","minecraft"] # input your keywords

df = collect_keywords(keywords, max_results_per_keyword=100, start_year=2020)

csv_save(df, "ytraw.csv")

df.head() # can be changed to preview data in different ways