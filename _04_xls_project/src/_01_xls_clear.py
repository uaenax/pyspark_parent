from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("完成新零售的相关需求")
    spark = SparkSession.builder.master('local[*]').appName('xls_clear').getOrCreate()
    df_init = spark.read.csv(
        path='file:///export/data/workspace/pyspark_parent/_04_xls_project/data/E_Commerce_Data.csv',
        header=True,
        inferSchema=True
    )
    # 对数据进行清理
    df_clear = df_init.where('CustomerID  != 0 and Description is not null')
    df_clear = df_clear.withColumn(
        'InvoiceDate',
        F.from_unixtime(F.unix_timestamp(df_clear['InvoiceDate'], 'M/d/yyyy H:m'), 'yyyy-MM-dd HH:mm')
    )
    # 将数据写进hdfs
    df_clear.write.mode('overwrite').csv(
        path='hdfs://node1:8020/xls/output',
        sep='\001'
    )
    # 关闭spark对象
    spark.stop()

