"""Api class for accessing Apterra resources."""
import os
import requests
from urllib.parse import urljoin
from typing import Optional

from datetime import datetime, timedelta, timezone
from utils import *

# Parameters, to be set on front end
# Q1 = 1.0
# Q2 = 15.0
# Q3 = 50
# TOPIC = "apterra-00001"
# LOCATION = "Kilembe"
# INTERVAL = 900

def post_notify(base_url: str = "https://ntfy.sh", topic: Optional[str] = None, msg: Optional[str] = None):
    """Send a ntfy.sh message."""
    url = urljoin(base_url, topic)
    msg_enc = msg.encode(encoding="utf-8")
    requests.post(
        url,
        data=msg_enc
    )

def post_message(q1, q2, q3, topic, t: datetime, loc: str, q: float, h: float, video_link: str) -> None:
    """Make a message dependent on discharge thresholds exceeded or not, and post in ntfy channel."""
    if q3 is not None and q >= q3:
        print("Warning level DANGER exceeded, sending notification...")
        template = orc_danger_msg_template
    elif q2 is not None and q >= q2:
        print("Warning level CAUTION exceeded, sending notification...")
        template = orc_caution_msg_template
    elif q1 is not None and q >= q1:
        print("Warning level NOTE exceeded, sending notification...")
        template = orc_notification_template
    else:
        template = None
    t_str = t.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    if template:
        msg = template(t=t_str, location=loc, q=q, h=h, video_link=video_link)
        # push to ntfy
        # print(msg)
        post_notify(topic=topic, msg=msg)

def get_msg_fields(ts, loc: str) -> dict:
    """Create a set of fields to pass to ntfy messenger."""
    # start with the video link, needs to be constructed from the configured LiveORC details, combined with video details.
    with get_session() as session:
        # get the callback url to form the video_link
        cb = crud.callback_url.get(session)
        if cb:
            url = cb.url
            site_id = cb.remote_site_id
            video_link = f"{url}admin/api/video/{ts.video.remote_id}/"
        else:
            video_link = ""
    t = ts.timestamp
    if ts.q_50:
        q = round(ts.q_50, 3)
    else:
        q = -999  # setting to 0.0 should never lead to ntfy messaging. It means no solution was found for the video and thus no message will be transmitted.
    h = round(ts.h, 3)
    return {
        "t": t,
        "loc": loc,
        "q": q,
        "h": h,
        "video_link": video_link
    }

"""Notification message."""
orc_notification_template = """
🌊 NOTE: Rising discharge detected at {t} at {location}
Discharge is {q} m³/s with a water level of {h} m.
For the video analysis please click here:
{video_link}
""".format

"""Warning message template."""
orc_caution_msg_template = """
⚠️ CAUTION: High discharge detected at {t} at {location}
Discharge is {q} m³/s with a water level of {h} m.
For the video analysis please click here:
{video_link}
""".format

"""Danger message template."""
orc_danger_msg_template = """
🚨 DANGER: VERY High discharge detected at {t} at {location}
Discharge is {q} m³/s with a water level of {h} m.
For the video analysis please click here:
{video_link}
""".format


#  try out
# msg = orc_warning_msg_template(t=datetime.now(tz=timezone.utc), location="Kilembe", q=320.67, h=5.27, video_link=)
#
# print("Posting message to NTFY.sh")
# post_notify(topic="apterra-00001", msg=msg)
