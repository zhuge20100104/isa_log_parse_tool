# ISA Log Parser Tool

## 用法

  python {target_log} {expect_log} {ISA_报文.csv}

  - target_log: 新生成的需要对此的log
  - expect_log: 之前的老log
  - ISA_报文.csv: ISA报文对应的csv文件

## 作用

  - 比较新log和老log都有什么不同，不同的部分在diff.csv文件中展示如果没有不同，则无输出。界面会显示 compare successfully。
  - 输出一个loc.json文件，里面有50个随机采样的点，和对应的数据详情。 