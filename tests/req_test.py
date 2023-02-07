import requests
import json 
from collections import namedtuple 

Speeds = namedtuple("Speeds", ["maxspeed", "maxspeed_forward", "maxspeed_backward"])

url = "http://mapilot.telenav.com/maps/unidb/query_by_latlon/?point={}%2C{}&naveType=both&timestamp=1675667501000&dbhost={}&dbname={}&region={}".format(
    41.388661400000004, 2.1070712, 
    "hqd-ssdpostgis-06.mypna.com", 
    "UniDB_HERE_EU22Q1_a00d200_20220119021805_RC",
    "EU"
)

res = requests.get(url)
data_ = json.loads(res.text)

metrics = data_["data"]["tags"]

maxspeed = None 
maxspeed_f = None 
maxspeed_b = None
for metric in metrics:
    print(metric)
    if "maxspeed" in metric:
        maxspeed = metric[1]
    elif "maxspeed:backward" in metric:
        maxspeed_b = metric[1]
    elif "maxspeed:forward" in metric:
        maxspeed_f = metric[1]
speeds = Speeds(maxspeed, maxspeed_f, maxspeed_b)

print(speeds)