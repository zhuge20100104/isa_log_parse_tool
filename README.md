# ISA Log Parser Tool

## 用法
  ISA Log日志解析工具。

  该工具分为两个脚本，一个是负责对比新的日志和老的日志的区别的，名为[isa_log_parser.py]。第一个脚本的输出结果为 [loc_and_data_diff.xlsx]。

  另一个是负责 使用 第一个脚本 输出的结果[f_loc.json]， 使用mapilot接口获取maxspeed,并和log日志中的maxspeed进行对比的，名为 isa_mapilot_compare.py。第二个脚本的输出结果为[mapilot_compare_result.xlsx]。

  必须先运行 [isa_log_parser.py]，生成对应的中间产物[f_loc.json]，才能接着运行[isa_mapilot_compare.py]，进行逐一比较。

### 前提
  本工程依赖于开发的ISAToolkit.jar包，并且只能在Windows平台运行。
  
  不能在Linux平台运行的原因是，开发的ISAToolkit.jar包在Linux平台上测试运行有bug(若要支持Linux平台的Jenkins node，请找XinHeng Shen先看一下jar包的问题)。
  
  另一个问题是可能py调用ISAToolkit部分也不支持Linux平台，这个部分需要找我适配。

  开发承诺后续会继续维护ISAToolKit.jar包，如果有新的jar包版本，请下载对应的jar包，放到当前工程的根目录下，并且保证名字为 [ISAToolkit.jar]，以避免发生找不到jar包的现象。

  如题所述，ISAToolkit.jar依赖JDK才能运行，请保证本机安装有JDK 1.8以上。

  Python部分依赖 python 3.7及以上，不支持已退休的Python 2.7系列。

  请先安装 对应的py依赖。

  ```shell
  pip install -r requirements.txt
  ```




### isa_log_parser.py用法

  python isa_log_parser.py {target_log_folder} {expect_log_folder} {csv_file_path} {way}

  - target_log_folder: 新生成的需要对比的log目录，例如"\\qadata\shqa\Qiankun\logs for comparison\22Q4_0.7.1.0003_0202_log",
  - expect_log_folder: 之前的老log目录，例如"\\qadata\shqa\Qiankun\logs for comparison\22Q3_0.7.1.0003_0202_log",
  - csv_file_path: ISA报文对应的csv文件,最好保存成一个英文名的csv名字。
  - way: 比较日志的方法，一共有两种，用于扩展的。一种是old，一种是qiankun，推荐使用qiankun即可。

  示例用法

  ```shell
     python isa_log_parser.py "\\qadata\shqa\Qiankun\logs for comparison\22Q4_0.7.1.0003_0202_log" "\\qadata\shqa\Qiankun\logs for comparison\22Q3_0.7.1.0003_0202_log" ./ISAMapReportsReq.csv qiankun
  ```

  运行此脚本之后会输出[f_loc.json] 和 [loc_and_data_diff.xlsx]。

### isa_mapilot_compare.py用法

  python isa_mapilot_compare.py  {dbhost} {dbname} {region} {thread_count}

  - dbhost: 数据库host地址
  - dbname: 数据库名
  - region: 区域
  - thread_count: 开启多少个线程进行服务端数据抓取，因为服务端服务器的吞吐量限制，大于一定数量时会出现SQL数据库查询错误。推荐值为100。

  示例用法

  ```shell
    python isa_mapilot_compare.py  hqd-ssdpostgis-06.mypna.com UniDB_HERE_EU22Q4_034c80e_20221011181047_RC EU 100
  ```

  运行此脚本之后会输出[mapilot_compare_result.xlsx]。就是对应的数据点的当前速度与mapilot接口中返回的速度的一个对比。



## 输出

  ### f_loc.json

  请看下图
  ![f_loc](/images/f_loc.png)

  ### loc_and_data_diff.xlsx
  
  请看下图
  ![loc_and_data_diff](/images/loc_and_send_data_diff.png)

  ### mapilot_compare_result.xlsx

  请看下图
  ![mapilot_compare_result](/images/mapilot_compare_result.png)




