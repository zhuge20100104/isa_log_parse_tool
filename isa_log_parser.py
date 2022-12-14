import sys 
import queue
import json
from collections import namedtuple

LocDataStr = namedtuple("LocDataStr", ["loc_s", "data_s"])
LocData = namedtuple("LocData", ["loc", "data"])
SendData = namedtuple("SendData", ["timestamp", "data"])
Loc = namedtuple("Loc", ["timestamp", "loc"])


class LogFileParser(object):
    def __init__(self, _log_path):
        self.log_path = _log_path
    
    def parse_str(self):
        send_data_list = list()
        loc_q = queue.Queue(-1)
        loc_and_data_list = list()

        with open(self.log_path) as log_f:
            for log_line in log_f:
                if log_line.strip() == "":
                    continue
                if log_line.strip().find("sendData:") != -1:
                    send_data_list.append(log_line.strip())
                    if not loc_q.empty():
                        loc_s = loc_q.get()
                        data_s = log_line.strip()
                        lds = LocDataStr(loc_s, data_s)
                        loc_and_data_list.append(lds)
                elif log_line.strip().find("location map matched from EHP") != -1:
                    loc_q.put(log_line.strip())
        print("Send data list len: " + str(len(send_data_list)))
        print("Loc and data list len: " + str(len(loc_and_data_list)))
        return send_data_list, loc_and_data_list

    def parse_send_data_s(self, send_data_s):
        '''2022-11-22 10:04:28.724 [SendIsaDataToAdasPool] INFO  ISASendLogger - [sendIsaDataToAdas]result:0,sendData:[31, 2, 2, 0, 8191, 0, 0, 0, 0, 255, 1, 2]'''
        time_stamp = send_data_s.split("[SendIsaDataToAdasPool]")[0]
        time_stamp = time_stamp.strip()
        data = send_data_s.split("sendData:")[1]
        data = data.strip()
        data_js = json.loads(data)
        return SendData(time_stamp, data_js)
    
    def parse_loc_s(self, loc_s):
        '''2022-11-22 10:04:35.802 [Thread-8] INFO  ISASendLogger - [MMFeedback] location map matched from EHP: (41.39435, 2.149)'''
        time_stamp = loc_s.split("[Thread-")[0]
        time_stamp = time_stamp.strip()
        loc = loc_s.split("location map matched from EHP:")[1]
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
 

class CSVValueParser(object):
    def __init__(self, _csv_path):
        self.csv_path = _csv_path
    
    def parse(self):
        pass

class LogParser(object):
    def __init__(self, _log_path, _csv_path):
        self.log_path = _log_path
        self.csv_path = _csv_path
    
    def parse(self):
        log_f_parser = LogFileParser(self.log_path)
        send_data_ls, loc_and_data_ls = log_f_parser.parse()
        print("Send data list: ")
        print(send_data_ls)
        print("===============================")
        print("Loc data list: ")
        print(loc_and_data_ls)
    

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python isa_log_parser.py {log_file_path} {csv_file_path}")
        print("Example: python isa_log_parser.py ./sendisa.log ./ISA地图报文申请1021.csv")
        exit(-1)
    log_path = sys.argv[1]
    csv_path = sys.argv[2]
    log_parser = LogParser(log_path, csv_path)
    log_parser.parse()
