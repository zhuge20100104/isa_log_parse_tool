from subprocess import Popen, PIPE 
import time 
import os 
from datetime import datetime
import re
from .funcs import  parse_key_val_pair
import io 
from .csv_value_parser import CSVValueParser
from .beans import *
import json

class QiankunLogParser(object):
    def __init__(self, log_folder, csv_path, parse_way, result_path):
        self.log_folder = log_folder
        self.csv_path = csv_path
        self.parse_way = parse_way
        self.result_path = result_path
        self.log_mapping = {
            "qiankun": 1
        }

    
    def convert_using_isa_toolkit(self):
        p = Popen(["java", "-jar", "ISAToolkit.jar"], stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
        p.stdin.write(b"2\r\n")
        time.sleep(1)
        parse_way_ = self.log_mapping[self.parse_way]
        parse_s = bytes("{}\r\n".format(parse_way_), 'utf-8')
        p.stdin.write(parse_s)
        time.sleep(1)
        log_folder_s = bytes("{}\r\n".format(self.log_folder), 'utf-8')
        p.stdin.write(log_folder_s)
        res, err =p.communicate()
        print(res)
        print(err)
        assert err == b"", "Convert to result.txt using ISA toolkit failed..."
    
    def parse_one_line(self, line):
        line_g = re.match(".*mm=(.*)\|.*", line)
        mm_gps_msg = line_g.group(1)

        # MMGPS(latLon=LatLon(lat=41.3889967, lon=2.156348), adasPosition=AdasPosition(positionMessage=2309760826875068646,
        #  canMsg=CanMsg(data=[10, 2, 2, 0, 8191, 1, 0, 0, 2, 197, 1, 1], elapsedTime=15739200))))
        lat_lon_g = re.match(".*lat=(.*)\, lon=(.*)\).*positionMessage=(.*)\,.*data=(\[.*\]).*", mm_gps_msg)
        lat = lat_lon_g.group(1)
        lon = lat_lon_g.group(2)
        pos_msg = lat_lon_g.group(3)
        data_str = lat_lon_g.group(4)

        loc_str = "[{}, {}]".format(lat, lon)
        loc = Loc(pos_msg, json.loads(loc_str))
        data = SendData(pos_msg, json.loads(data_str))
        return LocData(loc, data)

    def parse(self):
        self.convert_using_isa_toolkit()
        loc_and_data_ls = list()
        with io.open("result.txt", mode='r', encoding="utf-8") as result_f:
            for line in result_f.readlines():
                # Check if sendData canMsg exist in the current record
                if line.find("CanMsg") != -1:
                    loc_data = self.parse_one_line(line)
                    loc_and_data_ls.append(loc_data)

        csv_parser = CSVValueParser(self.csv_path)
        sign_value_map_list = csv_parser.parse()

        loc_and_details_list = list()
        for loc_and_data in loc_and_data_ls:
            loc_ = loc_and_data.loc 
            data = loc_and_data.data
            data_ = data.data
            details_ = list()
            for idx, val in enumerate(data_):
                sign_value_map = sign_value_map_list[idx]
                detail = parse_key_val_pair(val, sign_value_map)
                details_.append(detail)
            loc_and_details_list.append(LocAndSignDetails(loc_, details_))
        
        if os.path.exists(self.result_path):
            os.remove(self.result_path)
        os.rename("result.txt", self.result_path)
        return None, loc_and_details_list, loc_and_data_ls
