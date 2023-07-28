from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("读取外部文件的方式构建DF")
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()
    # 读取数据
    # 采用text的方式来读取数据,仅支持产生一列数据,默认列名为 value,当然可以通过schema修改列名
    df_init = spark.read\
        .format('text')\
        .schema(schema='id string')\
        .load('file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/stu.csv')

    df_init.printSchema()
    df_init.show()
