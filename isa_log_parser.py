# encoding: utf-8
import sys 
from utils.log_compare import LogCompare 
from utils.funcs import Printer


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: python isa_log_parser.py {target_file} {expect_file} {csv_file_path} {way}")
        print('Example: python isa_log_parser.py "\\qadata\shqa\Qiankun\logs for comparison\22Q4_0.7.1.0003_0202_log" "\\qadata\shqa\Qiankun\logs for comparison\22Q3_0.7.1.0003_0202_log" ./ISAMapReportsReq.csv qiankun')
        print('or for the old way:')
        print('Example: python isa_log_parser.py .\sendisa.2023-02-02.log .\sendisa.2023-01-19.log .\ISAMapReportsReq.csv old')
        print("The ways parameters include 'old/qiankun', etc.")
        print(": old indicates the way parse loc and senddata only from log files")
        print(": qiankun represents the way parse loc and senddata from navlog, adaslog and sendisa log files")
        exit(-1)

    target_file = sys.argv[1]
    expect_file = sys.argv[2]
    csv_path = sys.argv[3]
    parse_way = sys.argv[4]

    Printer.print_delimeter("Start to extract the log data...")
    log_compare = LogCompare(target_file, expect_file, csv_path, parse_way)
    log_compare.get_data()
    # The old way, only compares the send data one by one, requirements raised by XinHeng, Shen
    if parse_way == 'old':
        Printer.print_delimeter("Perform the old pure 'Send data' comparision way...")
        compare_res = log_compare.compare_data_list()
        if not compare_res:
            err_msg = "target file: %s is not matched with the expect file: %s" % (
                target_file, expect_file
            )
            print(err_msg)
        else:
            print("Compared successfully...")
    
    
    Printer.print_delimeter("Location and send data union comparison...")
    compare_res = log_compare.compare_loc_and_send_data()
    if not compare_res:
        err_msg = "target file: %s is not matched with the expect file: %s" % (
                target_file, expect_file
            )
        print(err_msg)
    else:
        print("Compared successfully...")

    Printer.print_delimeter("Start to save data...")
    log_compare.save_data()
    
