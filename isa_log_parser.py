# encoding: utf-8
import os
import sys 
import queue
import json
import random
from collections import namedtuple
import pandas as pd


LOC_SIZE = 50

LocDataStr = namedtuple("LocDataStr", ["loc_s", "data_s"])
LocData = namedtuple("LocData", ["loc", "data"])
SendData = namedtuple("SendData", ["timestamp", "data"])
Loc = namedtuple("Loc", ["timestamp", "loc"])

ExceedInfo = namedtuple("ExceedInfo", ["exceed", "value"])
SignValueMap = namedtuple("SignValueMap" , ["sign_name", "value_map", "exceed"])

Detail = namedtuple("Detail", ["sign_name", "key", "value"])
LocAndSignDetails = namedtuple("LocAndSignDetails", ["loc", "details"])


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
 


class Keys(object):
    StartKey = ''',,"'''
    EndKey = '''"'''
    EndValue = '''MPC,Unsigned'''
    EndValue_reserved = '''预留'''

class State(object):
    Unknown = 1
    StartKey = 2
    EndKey = 3
    StartValue = 4
    EndValue = 5
   

# SignalValueStrs = namedtuple("SignalValueStrs", ["signal_name", "value_strs"])
class SignalValueStrs(object):
    def __init__(self, _signal_name, _value_strs):
        self.signal_name = _signal_name
        self.value_strs = _value_strs
  
class CSVValueParser(object):
    def __init__(self, _csv_path):
        self.csv_path = _csv_path
    
    def parse_sign_value_strs(self):
        sign_value_s_list = list()
        state = State.Unknown
        with open(self.csv_path) as csv_f:
            for line in csv_f:
                if line.find(Keys.StartKey) != -1:
                    state = State.StartKey
                    half_key = line.split(Keys.StartKey)[1].strip() + " "
                    sign_val_strs = SignalValueStrs(half_key, list())
                elif state == State.StartKey and line.find(Keys.EndKey) != -1:
                    state = State.StartValue
                    # Two conditions, has MPC,Unsigned
                    if line.find(Keys.EndValue) != -1:
                        half_key_and_val1 = line.split(Keys.EndKey)
                        # Resolve the whole signal name
                        sign_val_strs.signal_name += half_key_and_val1[0].strip()
                        value = half_key_and_val1[-1]
                        value_1 = value.split(",")[1]
                        sign_val_strs.value_strs.append(value_1.strip())
                    # Not have MPC,Unsigned
                    else:
                        half_key_and_val1 = line.split(Keys.EndKey)
                        # Resolve the whole signal name
                        sign_val_strs.signal_name += half_key_and_val1[0].strip()
                        # Resolve the value strs[zero]
                        sign_val_strs.value_strs.append(half_key_and_val1[-1].strip())
                       
                elif state == State.StartValue:
                    # Not find end value sign
                    if line.find(Keys.EndValue) == -1 and line.find(Keys.EndValue_reserved) == -1:
                        sign_val_strs.value_strs.append(line.strip())
                    elif line.find(Keys.EndValue) != -1:
                        # Process end value status
                        # Set state to State.Unknown to start a new finding state
                        state = State.Unknown
                        end_value = line.split(Keys.EndKey)[0]
                        sign_val_strs.value_strs.append(end_value.strip())
                        sign_value_s_list.append(sign_val_strs)
                    elif line.find(Keys.EndValue_reserved) != -1:
                        state = State.Unknown
                        end_value = line.split(",")[3]
                        sign_val_strs.value_strs.append(end_value.strip())
                        sign_value_s_list.append(sign_val_strs)
        return sign_value_s_list

    def parse_one_value(self, value_, value_map):
        exceed = None
        if value_.find("：") != -1:
            k_v = value_.split("：")
        elif value_.find(":") != -1:
            k_v = value_.split(":")
        elif value_.find(" ") != -1:
            k_v = value_.split(" ")
        key = k_v[0]
        val = k_v[1]
        # 16-based number
        if key.find("-") != -1:
            start_and_end_key = key.split("-")
            start_key = int(start_and_end_key[0], 16)
            if start_and_end_key[1].find("0xXX") != -1:
                key = ">=" + str(start_key) 
                exceed = ExceedInfo(start_key, val)
            else:
                end_key = int(start_and_end_key[1], 16)
                for i in range(start_key, end_key + 1):
                    value_map[i] = val  
        # 16-based number  
        elif key.find("0x") != -1 or key.find("0X") != -1:
            int_k = int(key.strip(), 16)
            value_map[int_k] = val
        # 10-based number
        else:
            int_k = int(key.strip())
            value_map[int_k] = val
        return value_map, exceed

    def parse_sign_value_str_to_value_map(self, sign_value_s_list):
        sign_value_m_list = list()
        for sign_value in sign_value_s_list:
            g_exceed = None
            sign_name = sign_value.signal_name
            value_strs = sign_value.value_strs
            value_map = dict()
            # RampOffset is an exception
            if sign_name.find("RampOffset") != -1:
                for i in range(0, 8191):
                    value_map[i] = i 
                value_map[8191] = "Invalid"
            else:
                for value_s in value_strs:
                    values = list()
                    value_ = value_s

                    if value_s.find(",,,") != -1:
                        value_ = value_s.split(",")[3].strip()
                    elif value_s.find(",") != -1:
                        values = value_.split(",")
                    if len(values) == 0:
                        values.append(value_)

                    for val in values:
                        value_map, exceed = self.parse_one_value(val, value_map)
                        if exceed is not None:
                            g_exceed = exceed
                        values.clear()
                    
            sign_value_map = SignValueMap(sign_name, value_map, g_exceed)
            sign_value_m_list.append(sign_value_map)
        print("Sign value map list: " + str(len(sign_value_m_list)))
        return sign_value_m_list

    def parse(self):
        sign_value_s_list = self.parse_sign_value_strs()
        return self.parse_sign_value_str_to_value_map(sign_value_s_list)


class LogParser(object):
    def __init__(self, _log_path, _csv_path):
        self.log_path = _log_path
        self.csv_path = _csv_path
    
    def parse_key_val_pair(self, val, sign_value_map):
        val_map = sign_value_map.value_map
        sign_name = sign_value_map.sign_name
        exceed = sign_value_map.exceed
        if val in val_map:
            return Detail(sign_name, val, val_map[val])
        else:
            if exceed is not None: 
                exceed_val = exceed.exceed
                value = exceed.value
                if val >= exceed_val:
                    return Detail(sign_name, val, value)
                else:
                    return Detail(sign_name, val, "Parse_Unknown")
            else:
                return Detail(sign_name, val, "Parse_Unknown")

    def parse(self):
        log_f_parser = LogFileParser(self.log_path)
        send_data_ls, loc_and_data_ls = log_f_parser.parse()
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
                detail = self.parse_key_val_pair(val, sign_value_map)
                details_.append(detail)
            loc_and_details_list.append(LocAndSignDetails(loc_, details_))
        return send_data_ls, loc_and_details_list

class FailReasons(object):
    FailNumNotEq = "FailNumNotEq"
    FailDataNotMatch = "FailDataNotMatch"

class LogCompare(object):
    def __init__(self, _target_file, _expect_file, _csv_path):
        self.target_file = _target_file
        self.expect_file = _expect_file
        self.csv_path = _csv_path
        self.send_data_list_target = None 
        self.send_data_list_expect = None 
        self.loc_and_data_ls_target = None 
        self.loc_and_data_ls_expect = None
    
    def get_data(self):
        log_p_target = LogParser(self.target_file, self.csv_path)
        log_p_expect = LogParser(self.expect_file, self.csv_path)
        self.send_data_list_target, self.loc_and_data_ls_target = log_p_target.parse()
        self.send_data_list_expect, self.loc_and_data_ls_expect = log_p_expect.parse() 

    def compare_data_list(self):
        fail_reasons = list()
        if len(self.send_data_list_expect) != len(self.send_data_list_target):
            fail_reasons.append(FailReasons.FailNumNotEq)

        df_target = pd.DataFrame(self.send_data_list_target)
        df_target.rename(columns={"timestamp": "dst_timestamp"}, inplace=True)
        df_expect = pd.DataFrame(self.send_data_list_expect)

        df_target["data"] = df_target["data"].astype(str)
        df_expect["data"] = df_expect["data"].astype(str)
       
        df_res = df_target.merge(df_expect, on=["data"], how="left")
        print(df_res.head())
        rs = df_res[df_res["timestamp"].isna() | df_res["dst_timestamp"].isna()]
        print(rs.head())
        if os.path.exists("diff.csv"):
            os.remove("diff.csv")
        if len(rs) != 0:
            fail_reasons.append(FailReasons.FailDataNotMatch)
            rs.to_csv("diff.csv", encoding='utf-8')
        if len(fail_reasons) > 0:
            failed_msg = "Failed Reasons: "
            for fail_reason in fail_reasons:
                failed_msg += fail_reason
            print(failed_msg)
            return False
        return True

    def make_a_loc_js(self, loc_ele):
        loc = loc_ele.loc
        details = loc_ele.details
        result = dict()
        result["timestamp"] = loc.timestamp
        result["loc"] = loc.loc
        result["details"] = list()
        for detail in details:
            detail_js = dict()
            detail_js["sign_name"] = detail.sign_name
            detail_js["key"] = detail.key
            detail_js["value"] = detail.value
            result["details"].append(detail_js)
        return result
        
    def choose_random_data(self):
        loc_size = len(self.loc_and_data_ls_target)
        random.seed(10)
        i = 0
        print("loc_size: " + str(loc_size))
        res_json = list()
        while i < LOC_SIZE:
            loc_idx = random.randint(0, loc_size-1)
            loc_ele = self.loc_and_data_ls_target[loc_idx]
            loc_js = self.make_a_loc_js(loc_ele)
            res_json.append(loc_js)
            i += 1

        with open("loc.json", "w") as loc_f:
            res_str = json.dumps(res_json, indent=4, ensure_ascii=False)
            loc_f.write(res_str)
        
if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python isa_log_parser.py {target_file} {expect_file} {csv_file_path}")
        print("Example: python isa_log_parser.py ./sendisa.log ./sendisa1.log ./ISA地图报文申请1021.csv")
        exit(-1)

    target_file = sys.argv[1]
    expect_file = sys.argv[2]
    csv_path = sys.argv[3]

    log_compare = LogCompare(target_file, expect_file, csv_path)
    log_compare.get_data()
    compare_res = log_compare.compare_data_list()
    if not compare_res:
        err_msg = "target file: %s is not matched with the expect file: %s" % (
            target_file, expect_file
        )
        print(err_msg)
    else:
        print("Compared successfully...")
    
    log_compare.choose_random_data()
    
