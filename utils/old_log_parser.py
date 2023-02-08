from .old_log_file_parser import LogFileParser
from .beans import LocAndSignDetails
from .csv_value_parser import CSVValueParser
from .funcs import parse_key_val_pair

# The old log parser, returns location and data list, Deprecated
class LogParser(object):
    def __init__(self, _log_path, _csv_path):
        self.log_path = _log_path
        self.csv_path = _csv_path
    
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
                detail = parse_key_val_pair(val, sign_value_map)
                details_.append(detail)
            loc_and_details_list.append(LocAndSignDetails(loc_, details_))
        return send_data_ls, loc_and_details_list, loc_and_data_ls

