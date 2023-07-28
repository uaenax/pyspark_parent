import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("pySpark模板")
    spark = SparkSession.builder.master('local[*]').appName('测试题').getOrCreate()
    df_init = spark.read.csv(path='file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/唱跳rap和篮球.txt',
                             header=True, inferSchema=True)
    df_init.createTempView('t1')


    # 需求1:助攻这一列需要加10
    @F.pandas_udf(returnType=IntegerType())
    def add_zhu(a: pd.Series) -> pd.Series:
        return a + 10


    spark.udf.register('add_zhu_sql', add_zhu)
    df_init.select('助攻', add_zhu('助攻').alias('助攻+10')).show()


    # 需求2:篮球+助攻的次数
    @F.pandas_udf(returnType=IntegerType())
    def ikuu(a: pd.Series, b: pd.Series) -> pd.Series:
        return a + b


    spark.udf.register('ikuu_sql', ikuu)
    df_init.select('篮板', '助攻', ikuu('篮板', '助攻').alias('篮板+助攻')).show()


    # 需求3:统计胜和负的平均分
    @F.pandas_udf(returnType=IntegerType())
    def avg_ikuu(a: pd.Series) -> int:
       return a.mean()


    spark.udf.register('avg_ikuu_sql', avg_ikuu)
    spark.sql("""
        select 
            `胜负`,
            avg_ikuu_sql(`得分`)
        from t1
        group by `胜负`
    """).show()

    df_init.groupby('胜负').agg(avg_ikuu('得分')).show()
