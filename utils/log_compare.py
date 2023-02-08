from .old_log_parser import LogParser
from .qiankun_log_parser import QiankunLogParser
from .beans import FailReasons
from .data_saver import DataSaver
import pandas as pd
import json
import os


class LogCompare(object):
    def __init__(self, _target_file, _expect_file, _csv_path, parse_way):
        self.target_file = _target_file
        self.expect_file = _expect_file
        self.csv_path = _csv_path
        self.parse_way = parse_way
        self.send_data_list_target = None 
        self.send_data_list_expect = None 
        self.loc_and_data_ls_target = None 
        self.loc_and_data_ls_expect = None
        self.loc_and_data_ls_target_raw = None
        self.loc_and_data_ls_expect_raw = None
    
    def get_data(self):
        
        if self.parse_way == 'old':
            log_p_target = LogParser(self.target_file, self.csv_path)
            log_p_expect = LogParser(self.expect_file, self.csv_path)
        elif self.parse_way == 'qiankun':
            log_p_target = QiankunLogParser(self.target_file, self.csv_path, self.parse_way, "result_target.txt")
            log_p_expect = QiankunLogParser(self.expect_file, self.csv_path, self.parse_way, "result_expect.txt")
            
        self.send_data_list_target, self.loc_and_data_ls_target, \
            self.loc_and_data_ls_target_raw = log_p_target.parse()
        self.send_data_list_expect, self.loc_and_data_ls_expect, \
            self.loc_and_data_ls_expect_raw = log_p_expect.parse() 

    def compare_data_list(self):
        if self.send_data_list_expect is None or self.send_data_list_target is None:
            print("No data get, skip the comparision process...")
            return True
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
        print("Diff length: ", len(rs))
        if os.path.exists("diff.csv"):
            os.remove("diff.csv")
        if len(rs) != 0:
            fail_reasons.append(FailReasons.FailDataNotMatch)
            rs.to_csv("diff.csv", index=False, encoding='utf-8')
        if len(fail_reasons) > 0:
            failed_msg = "Failed Reasons: "
            for fail_reason in fail_reasons:
                failed_msg += fail_reason
                failed_msg += " "
            print(failed_msg)
            return False
        return True

    def compare_loc_and_send_data(self):
        fail_reasons = list()
        print("target len: " + str(len(self.loc_and_data_ls_target_raw)))
        print("expect len: " + str(len(self.loc_and_data_ls_expect_raw)))
        if len(self.loc_and_data_ls_target_raw) != len(self.loc_and_data_ls_expect_raw):
            fail_reasons.append(FailReasons.FailNumNotEq)
        target_df_ls = list()
        for target_ele in self.loc_and_data_ls_target_raw:
            one_target_ls = list()
            one_target_ls.append(target_ele.loc.timestamp)
            one_target_ls.append(target_ele.data.timestamp)
            one_target_ls += target_ele.loc.loc
            one_target_ls += target_ele.data.data
            target_df_ls.append(one_target_ls)
        expect_df_ls = list()    
        for expect_ele in self.loc_and_data_ls_expect_raw:
            one_expect_ls = list()
            one_expect_ls.append(expect_ele.loc.timestamp)
            one_expect_ls.append(expect_ele.data.timestamp)
            one_expect_ls += expect_ele.loc.loc
            one_expect_ls += expect_ele.data.data
            expect_df_ls.append(one_expect_ls)
        
        target_df = pd.DataFrame(target_df_ls)
        expect_df = pd.DataFrame(expect_df_ls)
        
        target_df.columns = ["t_pos_msg", "t_data_time", "x", "y", "t1", "t2", "t3", "t4", "t5", \
            "t6", "t7", "t8", "t9", "t10", "t11", "t12"]
        expect_df.columns = ["e_pos_msg", "e_data_time", "x", "y", "e1", "e2", "e3", "e4", "e5", \
            "e6", "e7", "e8", "e9", "e10", "e11", "e12"]

        print("Before drop target :", len(target_df))
        print("Before drop expect :", len(expect_df))
        target_df = target_df.drop_duplicates(['x', 'y'], keep='last')
        expect_df = expect_df.drop_duplicates(['x', 'y'], keep='last')

        print("After drop target :", len(target_df))
        print("After drop expect :", len(expect_df))

        res_df = pd.merge(target_df, expect_df, on=["x", "y"], how="left")
        res_df.insert(2, "loc", res_df["x"].astype(str) +", " + res_df["y"].astype(str))

        print("Len of joined result: ", len(res_df))
        
        def filter_func(x):
            res = x['t1'] != x['e1'] or x['t2'] != x['e2'] or \
                x['t3'] != x['e3'] or x['t4'] != x['e4'] or \
                x['t5'] != x['e5'] or \
                x['t6'] != x['e6'] or x['t7'] != x['e7'] or \
                x['t8'] != x['e8'] or x['t9'] != x['e9'] or \
                x['t10'] != x['e10'] or x['t11'] != x['e11'] or \
                      x['t12'] != x['e12']
            return res
        
        filter_idx = res_df.apply(filter_func, axis=1)
        res_df = res_df[filter_idx]

        res_df = res_df.dropna()

        def diff_func(x):
            res = dict()
            for i in range(1, 13):
                target_col = 't{}'.format(i)
                expect_col = 'e{}'.format(i)
                if x[target_col] != x[expect_col]:
                    res[target_col] =  x[target_col]
                    res[expect_col] = x[expect_col]
            return json.dumps(res)
        res_df.insert(3, "diff", res_df.apply(diff_func, axis=1))
        print("Result df len:", len(res_df))
        
        if len(res_df) != 0:
            fail_reasons.append(FailReasons.FailDataNotMatch)
            res_df.to_excel("loc_and_data_diff.xlsx", index=False)
        
        if len(fail_reasons) > 0:
            failed_msg = "Failed Reasons: "
            for fail_reason in fail_reasons:
                failed_msg += fail_reason
                failed_msg += " "
            print(failed_msg)
            print("Failed diff file is located at 'loc_and_data_diff.csv'")
            return False
        return True

    def save_data(self):
        target_saver = DataSaver(self.loc_and_data_ls_target)
        target_saver.save_data("loc_target.json")
        expect_saver = DataSaver(self.loc_and_data_ls_expect)
        expect_saver.save_data("loc_expect.json")