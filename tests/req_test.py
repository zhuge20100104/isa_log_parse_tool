import requests
import json 
from collections import namedtuple 

Speeds = namedtuple("Speeds", ["maxspeed", "maxspeed_forward", "maxspeed_backward"])

url = "http://mapilot.telenav.com/maps/unidb/query_by_latlon/?point={}%2C{}&naveType=both&timestamp=1675667501000&dbhost={}&dbname={}&region={}".format(
    41.347563, 2.112250,
    "hqd-ssdpostgis-06.mypna.com", 
    "UniDB_HERE_EU22Q4_034c80e_20221011181047_RC",
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