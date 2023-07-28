from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("WordCount案例: 方式一  RDD 转换为 DF 方式完成")
    # 创建SparkSession对象和sc对象
    spark = SparkSession.builder.master('local[*]').appName('wd_1').getOrCreate()
    sc = spark.sparkContext

    # 读取数据:采用 RDD方式
    rdd_init = sc.textFile('file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/word.txt')
    # 将数据转换为一个个的单词
    # 注意:从rdd转换为df要求列表中数据必须为元组
    rdd_word = rdd_init.flatMap(lambda line: line.split()).map(lambda word: (word,))
    # 将RDD转换df对象
    df_words = rdd_word.toDF(schema=['words'])
    # 完成wordcount案例

    # SQL
    df_words.createTempView('t1')
    spark.sql('select words,count(1) as cnt from t1 group by words').show()

    # DSL
    # withColumnRenamed() 修改列表:参数1表示旧列名 参数2表示新列名
    df_words.groupby('words').count().withColumnRenamed('count', 'cnt').show()
    df_words.groupby('words').agg(
        F.count('words').alias('cnt')
    )
    # 关闭资源
    spark.stop()
