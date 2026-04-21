"""Utility and helper functions to exchange data between Apterra and ORc-OS."""

import os
import requests

from datetime import datetime, timedelta, timezone
from typing import List
from urllib.parse import urljoin, urlparse

from orc_api.database import get_session
from orc_api import crud
from orc_api.db.video import Video

def find_closest_timeseries_orc(t: datetime, max_dt: timedelta = timedelta(minutes=15), load_video=True):
    """Find closest time series record to provided datetime with a timedelta tolerance."""
    # open a ORC-OS database session
    with get_session() as session:
        # get all time series that are within the allowed time span
        time_series = crud.time_series.get_list(
            session,
            start=t - max_dt,
            stop=t + max_dt,
        )
        if len(time_series) == 1:
            # a unique record was found, return that
            ts = time_series[0]
        elif len(time_series) > 1:
            # more records were found, we need to find the single closest record and return that.
            # find closest and return that
            closest = min(time_series, key=lambda r: abs(r.timestamp.replace(tzinfo=timezone.utc) - t))
            ts = closest
        else:
            ts = None
        if ts and load_video:
            # attach video instance by accessing it while the session is still open
            ts.video = ts.video
        return ts

def parse_time_from_url(url, prefix="video_", suffix=".mp4"):
    """Retrieve datetime from url"""
    # first split url and get last parts
    file = url.split("/")[-1]
    timestr = file.split(prefix)[1].split(suffix)[0]
    if "_" in timestr:
        # apparently a datetime string is used
        t = datetime.strptime(timestr, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
    else:
        t = datetime.fromtimestamp(float(timestr), timezone.utc)
    return t

    
def download_file(url: str, target_dir: str) -> str:
    """
    Download a file from a URL and save it to target_dir
    using the original filename.

    Args:
        url (str): Direct file URL (e.g., MinIO/S3 presigned URL)
        target_dir (str): Directory where file should be saved

    Returns:
        str: Full path to the downloaded file
    """

    # Ensure target directory exists
    os.makedirs(target_dir, exist_ok=True)

    # Extract filename from URL
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)

    if not filename:
        raise ValueError("Could not determine filename from URL")

    file_path = os.path.join(target_dir, filename)

    # Stream download to avoid loading whole file into memory
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    return file_path

def download_video(base_url: str, bucket: str, name: str, target_dir: str):
    """Download video with name derived directly from apterra to target directory with the same file name."""
    end_point = os.path.join(bucket, name)
    url = urljoin(base_url, end_point)
    print(f"Getting file from {url}")
    fn = download_file(url, target_dir=target_dir)
    print(f"File downloaded to {fn}")
    return fn

def scan_orc_video(name) -> List[str]:
    """check Video table of ORC-OS for the substring, return records."""
    # first get the last part
    fn = os.path.basename(name)
    with get_session() as session:
        query = crud.video.get_query_list(session)
        # filter out the one with a certain filename
        recs = query.where(Video.file.contains(fn)).all()
    session.close()
    return recs
    
def filter_orc_videos(videos):
    """check which videos already exist in the database, only return those not existing."""
    videos_filter = []
    # first get all records in query
    with get_session() as session:
        query = crud.video.get_query_list(session)
        # filter out the one with a certain filename

    for video in videos:
        fn = os.path.basename(video["name"])
        recs = query.where(Video.file.contains(fn)).all()
        if len(recs) == 0:
            # apparently record does not yet exist
            videos_filter.append(video)
    session.close()
    return videos_filter

