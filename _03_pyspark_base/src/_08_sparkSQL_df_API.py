from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示dataFrame的相关的API")
    spark = SparkSession.builder.master('local[*]').appName('create_df').getOrCreate()
    # 读取数据
    # 采用text的方式来读取数据,仅支持产生一列数据,默认列名为 value,当然可以通过schema修改列名
    df_init = spark.read.csv(path='file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/stu.csv',
                             header=True,
                             inferSchema=True)
    df_init.printSchema()
    df_init.show()
    # select操作:查看ID列和address列
    df_init.select('id', 'address').show()
    df_init.select(df_init['id'], df_init['address']).show()
    df_init.select([df_init['id'], df_init['address']]).show()
    df_init.select(['id', 'address']).show()

    # where和filter
    df_init.where(df_init['id'] > 2).show()
    df_init.where('id > 2').show()

    # group by :统计每个地区有多少个人
    df_init.groupby(df_init['address']).count().show()
    # 统计每个地区有多少个人以及有多少个不同年龄的人
    df_init.groupby('address').agg(
        F.count('id').alias('cnt'),
        F.countDistinct('age').alias('age_cnt')
    ).show()

    # SQL
    df_init.createTempView('t1')
    spark.sql('select address,count(id) as cnt,count(distinct age) as age_cnt from t1 group by address').show()
