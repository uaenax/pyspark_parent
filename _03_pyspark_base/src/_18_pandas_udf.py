from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pandas as pd
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("基于pandas定义UDF函数")
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()
    df_init = spark.createDataFrame([(1, 3), (2, 5), (3, 8), (5, 4), (6, 7)], schema='a int,b int')
    df_init.createTempView('t1')


    # 处理数据
    # 需求:基于pandas的udf完成对 a 和 b列乘积计算
    # 自定义一个Python的函数:传入series类型,返回series类型
    @F.pandas_udf(returnType=IntegerType())
    def pd_cj(a, b):
        return a * b

    # 需求:pandas的UDAF需求 对B列 求和
    @F.pandas_udf(returnType=IntegerType())
    def pd_b_sum(b: pd.Series) -> int:
        return b.sum()


    # 对函数进行注册
    # 方式一
    pd_cj_dsl = spark.udf.register('pd_cj_sql', pd_cj)

    pd_b_sum_dsl = spark.udf.register('pd_b_sum_sql', pd_b_sum)

    # 使用自定义函数
    # SQL
    spark.sql("""
        select a,b,pd_cj_sql(a,b) as cj from t1
    """).show()

    spark.sql("""
        select 
            pd_b_sum_sql(b) as sum_b
        from t1
    """).show()

    # DSL
    # df_init.select('a', 'b', pd_cj('a', 'b').alias('cj')).show()
