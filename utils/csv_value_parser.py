from utils.beans import SignValueMap
import io
from .beans import ExceedInfo

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
        with io.open(self.csv_path, mode="r", encoding="utf-8") as csv_f:
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
