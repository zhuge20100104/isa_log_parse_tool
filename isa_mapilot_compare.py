import sys 
import io 
import json
import requests
import time

from concurrent.futures import ThreadPoolExecutor
import threading

from collections import namedtuple
Speeds = namedtuple("Speeds", ["maxspeed", "maxspeed_forward", "maxspeed_backward"])
SpeedResult = namedtuple("SpeedResult", ["res", "speed"])


class SpeedGet(object):
    def __init__(self, db_host, db_name, region):
        self.db_host = db_host
        self.db_name = db_name 
        self.region = region
        

    def get_speed(self, lat, lon):
        url = "http://mapilot.telenav.com/maps/unidb/query_by_latlon/?point={}%2C{}&naveType=both&timestamp=1675667501000&dbhost={}&dbname={}&region={}".format(
            lat, lon, 
            self.db_host,
            self.db_name,
            self.region
        )

        res = requests.get(url)
        data_ = json.loads(res.text)
        metrics = data_["data"]["tags"]
        maxspeed = None 
        maxspeed_f = None 
        maxspeed_b = None
        for metric in metrics:
            if "maxspeed" in metric:
                maxspeed = metric[1]
            elif "maxspeed:backward" in metric:
                maxspeed_b = metric[1]
            elif "maxspeed:forward" in metric:
                maxspeed_f = metric[1]
        
        maxspeed_f = int(maxspeed_f)  if maxspeed_f is not None \
                else None
        maxspeed_b = int(maxspeed_b) if maxspeed_b is not None \
            else None
        maxspeed = int(maxspeed) if maxspeed is not None \
            else None
        speeds = Speeds(maxspeed, maxspeed_f, maxspeed_b)
        return speeds
        


class SpeedCompare(object):
    def __init__(self, db_host, db_name, region) :
        self.db_host = db_host
        self.db_name = db_name 
        self.region = region
        self.speed_get = SpeedGet(self.db_host, self.db_name, self.region)

    def compare_speed_info(self, speed: Speeds, cur_speed):
        speed_f = speed.maxspeed_forward
        speed_b = speed.maxspeed_backward
        max_speed = speed.maxspeed
        if speed_f is None and speed_b is not None:
            if speed_b != max_speed:
                return False
            if cur_speed != speed_b:
                return False 
            return True
        
        if speed_f is not None and speed_b is None:
            if speed_f != max_speed:
                return False
            if cur_speed != speed_f:
                return False 
            return True

        if speed_f is None and speed_b is None:
            if max_speed is None:
                return False  
            if cur_speed != max_speed:
                return False
            return True
        
        if speed_f is not None and speed_b is not None:
            if speed_f != speed_b:
                return False 

            if max_speed != speed_f:
                return False 
            
            if cur_speed != max_speed:
                return False
            return True
        return True 

    def get_and_compare_one_data(self, loc_and_data):
        a_res = dict()
        a_res['pos_msg'] = loc_and_data['timestamp']
        loc = loc_and_data['loc']
        a_res['lat'] = loc[0]
        a_res['lon'] = loc[1]
        speed = self.speed_get.get_speed(loc[0], loc[1])
        a_res['max_speed_forward'] = speed.maxspeed_forward
        a_res['max_speed_backward'] = speed.maxspeed_backward
        a_res['max_speed'] = speed.maxspeed
        cur_speed = loc_and_data["details"][0]["key"] * 5
        res = self.compare_speed_info(speed, cur_speed)
        return SpeedResult(res, a_res)


    def compare(self): 
        res_list = list()
        start = time.time()

        feat_list = list()
        with io.open("loc.json", mode="r", encoding="utf-8") as f:
            loc_and_datas = json.load(f)
            count = int(len(loc_and_datas) / 5)
            for loc_and_data in loc_and_datas:
                # a_res = dict()
                # a_res['pos_msg'] = loc_and_data['timestamp']
                # loc = loc_and_data['loc']
                # a_res['lat'] = loc[0]
                # a_res['lon'] = loc[1]
                # speed = self.speed_get.get_speed(loc[0], loc[1])
                # a_res['max_speed_forward'] = speed.maxspeed_forward
                # a_res['max_speed_backward'] = speed.maxspeed_backward
                # a_res['max_speed'] = speed.maxspeed
                # cur_speed = loc_and_data["details"][0]["key"] * 5
                # res = self.compare_speed_info(speed, cur_speed)
                speed_res= self.get_and_compare_one_data(loc_and_data)
                if speed_res.res is False:
                    res_list.append(speed_res.speed)
        end = time.time()

        print("Process time", (end-start), " s")
        print("Wrong result list len: ", len(res_list))
        print("Result list:")
        print(res_list)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python isa_mapilot_compare.py  {dbhost} {dbname} {region}")
        print("Example: python isa_mapilot_compare.py  hqd-ssdpostgis-06.mypna.com UniDB_HERE_EU22Q1_a00d200_20220119021805_RC EU")
        exit(0)
    
    db_host = sys.argv[1]
    db_name = sys.argv[2]
    db_region = sys.argv[3]
    sc = SpeedCompare(db_host, db_name, db_region)
    sc.compare()

