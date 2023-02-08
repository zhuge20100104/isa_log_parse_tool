import json
import io


class DataSaver(object):

    def __init__(self, loc_and_data_ls):
        self.loc_and_data_ls = loc_and_data_ls

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
    
    def convert_ele_to_dict(self):
        full_json = list()
        for ele in self.loc_and_data_ls:
            loc_js = self.make_a_loc_js(ele)
            full_json.append(loc_js)
        return full_json

    def save_data(self, file_name):
        full_json = self.convert_ele_to_dict()        
        with io.open(file_name, "w", encoding="utf-8") as loc_f:
            res_str = json.dumps(full_json, indent=4, ensure_ascii=False)
            loc_f.write(res_str)