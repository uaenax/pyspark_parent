from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示WordCount案例实现方式二: 通过直接读取数据转换为DF处理")
    # 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('wd_2').getOrCreate()
    # 读取数据
    df_init = spark.read.text('file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/word.txt')
    # 处理数据

    # 纯DSL方案:
    # 首先先将数据转换为一列数据,一行为一个单词 explode(列表)
    df_words = df_init.select(F.explode(F.split('value', ' ')).alias('words'))
    # 分组求个数
    df_words.groupby('words').count().show()

    # DSL + SQL 混合
    # 首先先将数据转换为一列数据,一行为一个单词 explode(列表)
    df_init.createTempView('t1')
    df_words = spark.sql('select explode(split(value," ")) as words from t1')
    # 分组求个数
    df_words.groupby('words').count().show()

    # 纯SQL实现
    df_words = spark.sql('select explode(split(value," ")) as words from t1')
    df_words.createTempView('t2')
    spark.sql('select words,count(1) from t2 group by words').show()

    # 等同于
    spark.sql("""
        select words,count(1) as cnt from 
            (select explode(split(value ," ")) as words from t1) as t2
        group by words
    """).show()

    spark.sql("""
        select words,count(1) from t1 lateral view explode(split(value," ")) t2 as words
        group by words
    """).show()
