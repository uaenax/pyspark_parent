from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import pandas as pd

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示pandas DF 转换为 spark SQL DF")
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()
    pd_df = pd.DataFrame({'id': ['c01', 'c02', 'c03'], 'name': ['张三', '李四', '王五']})
    spark_df = spark.createDataFrame(pd_df)

    spark_df.show()
    spark_df.printSchema()
