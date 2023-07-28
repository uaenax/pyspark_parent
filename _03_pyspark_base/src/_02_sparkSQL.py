from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("spark SQL 入门案例")
    # 构建SparkSession对象
    spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('init_spark sql') \
        .getOrCreate()
    # 读取外部文件的数据
    df_init = spark.read.csv(path='file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/test.csv',
                             header=True,
                             sep=',',
                             inferSchema=True)
    # 获取数据和元数据信息
    df_init.show()
    df_init.printSchema()

    # 需求:将城市为北京的数据获取出来: DSL
    df_where = df_init.where("city = '北京'")
    df_where.show()

    # 使用SQL来实现
    df_init.createTempView('t1')
    spark.sql("select * from t1 where city = '北京'").show()

    # 关闭spark对象
    spark.stop()
