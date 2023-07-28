from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示清洗相关的API")
    spark = SparkSession.builder.master('local[*]').appName('cleanAPI').config("spark.sql.shuffle.partitions",
                                                                               "4").getOrCreate()
    df_init = spark.read.csv(path='file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/stu.csv',
                             sep=',',
                             header=True,
                             inferSchema=True
                             )
    # 去重API:df.dropDuplicates()
    df = df_init.dropDuplicates()
    df = df_init.dropDuplicates(['address', 'name'])

    # 删除null值数据: df.dropna()
    df = df_init.dropna()
    df = df_init.dropna(thresh=3)
    df = df_init.dropna(thresh=2, subset=['name', 'address', 'age'])

    # 替换null值:df.fillna()
    df = df_init.fillna('aaaa')
    df = df_init.fillna('aaa', subset=['name', 'address'])
    df = df_init.fillna(1, subset=['id', 'age'])
    df = df_init.fillna({'id': 0, 'name': '未知', 'age': 0, 'address': '未知'})

    df.printSchema()
    df.show()
