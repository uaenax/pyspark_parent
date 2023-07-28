from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window as win
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示spark SQL的窗口函数使用")
    spark = SparkSession.builder.master('local[*]').appName('window').getOrCreate()
    df_init = spark.read.csv(path='file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/pv.csv',
                             header=True,
                             inferSchema=True)
    # 处理数据操作:演示窗口函数
    df_init.createTempView('t1')
    # SQL
    spark.sql("""
        select 
            uid,
            datestr,
            pv,
            row_number() over(partition by uid order by pv desc) as rank1,
            rank() over(partition by uid order by pv desc) as rank2,
            dense_rank() over(partition by uid order by pv desc) as rank3
        from t1
    """).show()

    # DSL
    df_init.select(
        df_init['uid'],
        df_init['datestr'],
        df_init['pv'],
        F.row_number().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('rank1'),
        F.rank().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('rank1'),
        F.dense_rank().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('rank1')
    ).show()
