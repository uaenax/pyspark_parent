from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("方式一: 通过RDD转换为dataFrame")
    # 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()
    # 从spark中获取sparkContext对象
    sc = spark.sparkContext
    # 读取数据,获取rdd
    rdd_init = sc.parallelize([('c01', '张三', 20), ('c02', '李四', 15), ('c03', '王五', 26), ('c01', '赵六', 30)])
    # 通过rdd将c01的数据过滤
    rdd_filter = rdd_init.filter(lambda tup: tup[0] != 'c01')

    # 请将rdd转换dataFrame对象
    # 方案一
    schema = StructType() \
        .add('id', StringType(), True) \
        .add('name', StringType(), False) \
        .add('age', IntegerType())
    df_init = spark.createDataFrame(rdd_filter, schema=schema)
    # 打印结果
    df_init.printSchema()
    df_init.show()

    # 方案二
    df_init = spark.createDataFrame(rdd_filter, schema=['id', 'name', 'age'])
    # 打印结果
    df_init.printSchema()
    df_init.show()

    # 方案三
    df_init = spark.createDataFrame(rdd_filter)
    # 打印结果
    df_init.printSchema()
    df_init.show()

    # 方案四
    df_init = rdd_filter.toDF()
    # 打印结果
    df_init.printSchema()
    df_init.show()

    # 方案五
    df_init = rdd_filter.toDF(schema=schema)
    # 打印结果
    df_init.printSchema()
    df_init.show()

    # 方案五
    df_init = rdd_filter.toDF(schema=['id', 'name', 'age'])
    # 打印结果
    df_init.printSchema()
    df_init.show()
