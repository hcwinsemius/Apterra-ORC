"""Api class for accessing Apterra resources."""
import os
import requests

from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse

from utils import parse_time_from_url

class ApterraApi:
    def __init__(self, base_url, api_key):
        """Initialize a BSaffer API request instance with base url and api key
        """
        self.url = base_url
        self.api_key = api_key

    @property
    def headers(self):
        return {"Authorization": f"Bearer {self.api_key}"}
        
    def get(
        self,
        endpoint,
        data=None,
        json=None,
    ):
        """Perform GET request on end point with optional data."""
        data = {} if data is None else data
        json = {} if json is None else json
        url = urljoin(str(self.url), endpoint)
        return requests.get(url, data=data, headers=self.headers, timeout=5)

    def patch(self, endpoint, json=None, data=None, files=None, timeout=5, delay_retry=5):
        """Perform PATCH request on end point with optional data and files."""
        data = {} if data is None else data
        json = {} if json is None else json
        files = {} if files is None else files
        url = urljoin(str(self.url), endpoint)
        return requests.patch(
            url, headers=self.headers, data=data, json=json, files=files, timeout=timeout, allow_redirects=True
        )

    def post(self, endpoint, data=None, json=None, files=None, timeout=5):
        """Perform POST request on end point with optional data and files."""
        data = {} if data is None else data
        files = {} if files is None else files
        json = {} if json is None else json
        url = urljoin(str(self.url), endpoint)
        return requests.post(url, headers=self.headers, json=json, data=data, files=files, timeout=timeout)


    def get_projects(self):
        """Get list of devices, belonging to project."""

        url = urljoin(self.url, f"api/project")
        response = self.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(response.status_code)

    
    def get_devices_project(self, project_id):
        """Get list of devices, belonging to project."""

        url = urljoin(self.url, f"api/devices/project/{project_id}")
        response = self.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(response.status_code)

    
    def get_device_videos(self, device_id):
        url = urljoin(self.url, f"api/devices/{device_id}/videos")
        
        response = self.get(url)
        if response.status_code == 200:
            data = response.json()
        else:
            raise Exception(response.status_code)
        # add the time stamps
        timestamps = [parse_time_from_url(rec["name"]) for rec in data]
        # add the time stamp to each record
        for n, t in enumerate(timestamps):
            data[n]["timestamp"] = t
        # sort the records
        sorted_data = sorted(data, key=lambda r: r['timestamp'])
        return sorted_data
        
    def get_closest_video(self, device_id, t: datetime, max_dt: timedelta = timedelta(minutes=15)):
        # first retrieve all videos (sorted on time stamp)
        videos = self.get_device_videos(device_id)
        # 
        
        closest = min(videos, key=lambda r: abs(r['timestamp'] - t))
        result = closest if abs(closest['timestamp'] - t) <= max_dt else None
        return result

    def get_device_videos_start_end(self, device_id: str, start: datetime = None, end: datetime = None):
        """Get list of videos from apterra belonging to device, from start to end date."""
        videos = self.get_device_videos(device_id)
        # filter
        if start:
            videos = [v for v in videos if v["timestamp"] >= start]
        if end:
            videos = [v for v in videos if v["timestamp"] <= end]
        return videos

