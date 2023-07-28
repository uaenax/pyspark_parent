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
    # sep参数:设置csv文件中字段的分隔字符,默认为逗号
    # header参数:设置csv是否含有头信息 True 有 默认为 false
    # inferSchema参数:用于让程序自动推断字段的类型 默认为false 默认所有的类型都是string
    # encoding参数:设置对应文件的字符集 默认为 UTF-8
    df_init = spark.read\
        .format('csv')\
        .option('sep', ',')\
        .option('header', True)\
        .option('inferSchema', True)\
        .option('encoding', 'UTF-8')\
        .load('file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/stu.csv')

    df_init.printSchema()
    df_init.show()
