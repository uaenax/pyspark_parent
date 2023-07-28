from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("读取外部文件的方式构建DF")
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()
    # 读取数据
    df_init = spark.read\
        .format('json')\
        .load('file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/employee.json')
    # 查询所有数据
    df_init.printSchema()
    df_init.show()
    # 查询所有数据,并去除重复数据
    df_init.dropDuplicates().show()
    # 查询所有数据,打印时去除ID字段
    df_init.select('age', 'name').show()
    # 筛选出age>30的记录
    df_init.where('age > 30').show()
    # 将数据按age分组
    df_init.groupby('age').count().show()
    # 将数据按name升序排列
    df_init.orderBy('name').show()
    # 取出前三行数据
    df_init.show(3)
    # 查询所有记录的name列.并为其取名为username
    df_init.select('name').alias('username').show()
    # 查询年龄age的平均值
    df_init.agg(F.avg('age')).show()
    # 查询年龄age的最小值
    df_init.orderBy('age').limit(1).show()
