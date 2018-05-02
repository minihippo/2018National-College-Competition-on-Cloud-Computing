# 2018全国高校云计算应用创新大赛

### 最终排名：1 
  
repo为**技能赛**的赛题一：[面向大数据的高高效关联规则推荐算法](https://cloud.seu.edu.cn/contest/newsBroswer.html?newsID=164)  

购物篮数据集

| 记录数       | 项目数  | 最短长度 | 最长长度 | 平均长度 |
| ---------------- |------------- | ----------------- | -------- | ------- |
| 1,692,082 | 5,267,656 | 1 | 71,472 | 177 |

用户数据集

| 用户数       | 项目数  | 最短长度 | 最长长度 | 平均长度 |
| ---------------- |------------- | ----------------- | -------- | ------- |
| 330,244 | 1,080,203 | 4 | 1,195 | 130 |

采用的频繁项集挖掘算法为PFP-Growth  

比赛相关信息点[这里](https://cloud.seu.edu.cn/contest/index)

### 项目结构

　　本项目用scala语言编写，用maven组织。代码结构如下。
- 项目src/main/AR目录下存放源代码文件。
- main文件夹中存放频繁项集挖掘与关联规则生成与关联规则匹配与推荐分值计算这两个模块的代码。
- util包里FPTree、AssociationRules是频繁项集挖掘所必须的数据结构，FPNewDef是基于mllib的FP-Growth算法的优化版本。
- conf文件夹包含一个Conf类用于程序运行参数配置。

~~~shell
|____src
| |____main
| | |____resources
| | |____scala
| | | |____AR
| | | | |____Main.scala                 # main函数
| | | | |____main
| | | | | |____FPGrowth.scala           # 频繁项集挖掘与关联规则生成
| | | | | |____RecPartUserRdd.scala     # 关联规则匹配与推荐分值计算
| | | | |____conf
| | | | | |____Conf.scala                  # 配置参数类
| | | | |____util
| | | | | |____AssociationRules.scala     # 关联规则实体类 
| | | | | |____FPTree.scala          
| | | | | |____FPNewDef.scala             # PFP基于spark源码优化算法
~~~

### 编译方法

　　推荐使用maven编译打包本项目。编译打包命令为：

~~~shell
mvn clean install -Dmaven.test.skip=true　
~~~

### 运行命令

　　运行脚本如下所示，由于赛题中只给出了一个输入路径参数和一个输出路径参数，因此代码中读取输入路径为`input+'/D.dat'`和`input+'/U.dat'`，所以在输入路径文件夹中需要包含D.dat和U.dat文件。代码中输出路径为`output+'/Frep'`和`output+'/Rec'`。临时工作目录用于存储关联规则。（若如下脚本不能运行，请将参数配置部分的"."改为“-”，如“executor-memory”改为“executor.memory”）

```shell
${SPARK_HOME/bin}/spark-submit \ 
--master <test spark cluster master uri> \ 
--class AR.Main\
```

~~~shell
--executor-memory 20G \ 
--driver-memory 20G \
<your jar file path> \ 
hdfs://<输入文件夹路径> \ 
hdfs://<结果输出文件(夹)路径> \ 
hdfs://<临时工作目录路径>  (可选)--spark_excutor_cores 8等运行参数配置，具体参数如下表
~~~

　　根据运行环境，请按照如下方式进行配置，默认配置参数适配环境为7个worker，每个worker16核30G内存。以运行环境为7个worker，每个worker24个核60G内存为例，配置如下参数。

~~~shell
${SPARK_HOME/bin}/spark-submit \ 
--master <test spark cluster master url> \ 
--class AR.Main\
--executor-memory 20G \ 
--driver-memory 20G \
<your jar file path> \ 
hdfs://<输入文件夹路径> \ 
hdfs://<结果输出文件(夹)路径> \ 
hdfs://<临时工作目录路径> \
--numPartitionAB 336 \  # =AB部分运行总核数*8
--spark_executor_cores_AB 2 \ #cores_AB与memory_AB保持1:10的比例，如果跑不通请适当调整这个比例
--spark_executor_memory_AB 20g \
--spark_cores_max_AB 42 \ # 单个executor使用的核数*总executor数
--numPartitionCD 168 \ # =CD部分运行总核数
--spark_executor_cores_CD 8 \ #cores_CD与memory_CD，要求核数，内存可以降一点
--spark_executor_memory_CD 20g \
--spark_cores_max_CD 168 \ # 单个executor使用的核数*总executor数
--spark_parallelism_CD 672 \ # numPartitionCD*4
~~~

Conf类实现了运行参数的配置，在脚本中可配的参数如表所示，默认参数按照运行环境7个worker，每个worker16核30G内存配置。

| 参数名称                        | 参数解释          | 参数类型 | 默认值  |
| ------------------------------- | ----------------- | -------- | ------- |
| --appName                       | 程序运行名称      | String   | PASA_AR |
| --numPartitionAB                | AB部分分区数      | Int      | 168     |
| --numPartitionCD                | CD部分分区数      | Int      | 112     |
| --spark_memory_fraction         | spark内存参数     | Double   | 0.7     |
| --spark_memory_storage_Fraction | spark内存参数     | Double   | 0.3     |
| --spark_shuffle_spill_compress  | spark shuffle参数 | Boolean  | true    |
| --spark_memory_offHeap_enable | spark堆外存参数            | Boolean  | true   |
| --spark_memory_offHeap_size   | spark参数                  | String   | 5g     |
| --spark_executor_instances    | 程序创建executor个数       | Int      | 21     |
| --spark_driver_cores          | driver可用核数             | Int      | 24     |
| --spark_driver_memory         | driver可用内存             | String   | 20g    |
| --spark_executor_memory_AB    | AB部分一个executor可用内存 | String   | 25g    |
| --spark_executor_cores_AB     | AB部分一个executor的核数   | Int      | 3      |
| --spark_cores_max_AB          | AB部分最大使用核数         | Int      | 21     |
| --spark_executor_memory_CD    | CD部分一个executor可用内存 | String   | 15g    |
| --spark_executor_cores_CD     | CD部分一个executor核数     | Int      | 8      |
| --spark_cores_max_CD          | CD部分最大使用核数         | Int      | 112    |
| --spark_parallelism_CD        | CD部分并行度               | Int      | 448    |
