import redis
import os
from dateutil.parser import isoparse

ESRI_API_KEY = os.environ.get('ESRI_API_KEY')
redis_instance = redis.StrictRedis.from_url(
    os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
)

count_1030 = 7516800
count_1033 = 12096000
count_1079 = 12182400

counts = {
    "1030.0": count_1030,
    "1033.0": count_1033,
    "1079.0": count_1079
}

start_time = '2023-05-30T00:00:00'
end_time = '2023-11-05T23:59:59'

xstart = isoparse(start_time).timestamp()
xend = isoparse(end_time).timestamp()