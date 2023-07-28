from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示 spark on hive的集成, 代码连接")
    spark = SparkSession\
        .builder\
        .master('local[*]')\
        .appName('spark_hive')\
        .config('spark.sql.shuffle.partitions', '4')\
        .config('spark.sql.warehouse.dir', 'hdfs://node1:8020/user/hive/warehose')\
        .config('hive.metastore.uris', 'thrift://node1:9083')\
        .enableHiveSupport()\
        .getOrCreate()
    spark.sql('show databases').show()
