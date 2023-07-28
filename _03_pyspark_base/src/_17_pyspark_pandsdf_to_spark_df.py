from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("pySpark模板")
    spark = SparkSession.builder.master('local[2]').appName('udf_01').getOrCreate()

    # 开启arrow
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # 创建pandas的df对象
    pd_df = pd.DataFrame({'name': ['张三', '李四', '王五'], 'age': [20, 18, 15]})
    # 可以使用panda的API来对数据进行处理操作
    pd_df = pd_df[pd_df['age'] > 16]
    print(pd_df)
    # 将其转换为spark df
    spark_df = spark.createDataFrame(pd_df)
    # 可以使用spark的相关API处理数据
    spark_df = spark_df.select(F.sum('age').alias('sum_age'))
    spark_df.printSchema()
    spark_df.show()
    # 还可以将spark df 转换为pandas df
    pd_df = spark_df.toPandas()
    print(pd_df)
