from .beans import *
import json
import re

# This class is for the old Log file parser, deprecated
class LogFileParser(object):
    def __init__(self, _log_path):
        self.log_path = _log_path
    
    def parse_str(self):
        send_data_list = list()
        loc_stack = list()
        loc_and_data_list = list()

        with open(self.log_path) as log_f:
            for log_line in log_f:
                if log_line.strip() == "":
                    continue
                if log_line.strip().find("sendData:") != -1:
                    send_data_list.append(log_line.strip())
                    if len(loc_stack) > 0:
                        loc_s = loc_stack.pop()
                        data_s = log_line.strip()
                        lds = LocDataStr(loc_s, data_s)
                        loc_and_data_list.append(lds)
                elif log_line.strip().find("location map matched from EHP") != -1:
                    loc_stack.append(log_line.strip())
        print("Send data list len: " + str(len(send_data_list)))
        print("Loc and data list len: " + str(len(loc_and_data_list)))
        return send_data_list, loc_and_data_list

    def parse_send_data_s(self, send_data_s):
        '''2022-12-16 10:40:17.831 - [sendIsaDataToAdas][Before]RoadStatus(vehicleOffset=-2147483648, metadata=Metadata(countryCode=-1, speedUnit=0, mapStatus=2), current=[], next=[], positionMessage=null),sendData:[31, 2, 2, 0, 8191, 0, 0, 0, 0, 255, 1, 2],elapsedTime:58274309''' 
        m = re.match('^\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}\.\d{0,3}', send_data_s)
        time_stamp = m.group(0)
        time_stamp = time_stamp.strip()

        m = re.match('.*sendData:(\[.*\])', send_data_s)
        data = m.group(1)
        data = data.strip()
        data_js = json.loads(data)
        return SendData(time_stamp, data_js)
    
    def parse_loc_s(self, loc_s):
        '''2022-12-16 10:40:26.179 - [MMFeedback] location map matched from EHP: (41.39435, 2.149)'''
        m = re.match('^\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}\.\d{0,3}', loc_s)
        time_stamp = m.group(0)
        time_stamp = time_stamp.strip()

        m = re.match(".*location map matched from EHP:.*(\(.*\)).*", loc_s)
        loc = m.group(1)
        loc = loc.strip()  
        # (1, 1) to [1, 1] 
        loc_ele_s = "[" + loc[1:-1] + "]"    
        loc_ele = json.loads(loc_ele_s)
        return Loc(time_stamp, loc_ele)

    def parse_ele(self, send_data_s_list, loc_and_data_s_list):
        send_data_ls = list()
        loc_and_data_ls = list()
        for send_data_s in send_data_s_list:
            send_data = self.parse_send_data_s(send_data_s)
            send_data_ls.append(send_data)

        for loc_and_data_s in loc_and_data_s_list:
            loc_s = loc_and_data_s.loc_s
            data_s = loc_and_data_s.data_s
            loc = self.parse_loc_s(loc_s)
            data = self.parse_send_data_s(data_s)
            loc_and_data_ls.append(LocData(loc, data))
        return send_data_ls, loc_and_data_ls

    def parse(self):
        send_data_str_list, loc_and_data_str_list = self.parse_str()
        return self.parse_ele(send_data_str_list, loc_and_data_str_list)