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
    print("演示spark sql的UDF函数: 返回列表/字典")
    spark = SparkSession.builder.master('local[*]').appName('udf_02').config("spark.sql.shuffle.partitions",
                                                                             4).getOrCreate()
    df_init = spark.read.csv(path='file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/user.csv',
                             header=True,
                             inferSchema=True)
    df_init.createTempView('t1')

    # 处理数据
    # 需求:自定义函数 请将line字段切割开,将其转换为 姓名 住址 年龄
    # 定义一个普通Python函数
    def split_3col_1(line):
        return line.split("|")


    def split_3col_2(line):
        arr = line.split('|')
        return {'name': arr[0], 'address': arr[1], 'age': arr[2]}

    # 注册
    # 对于返回列表的注册方式
    # 方式一
    scheam = StructType().add('name', StringType()).add('address', StringType()).add('age', StringType())
    split_3col_1_dsl = spark.udf.register('split_3col_1_sql', split_3col_1, scheam)

    # 对于返回字典的方式
    # 方式二
    split_3col_2_dsl = F.udf(split_3col_2, scheam)

    # 使用自定义函数
    spark.sql("""
        select 
            userid,
            split_3col_1_sql(line) as 3col,
            split_3col_1_sql(line)['name'] as name,
            split_3col_1_sql(line)['address'] as address,
            split_3col_1_sql(line)['age'] as age
        from t1
    """).show()
    # DSL中
    df_init.select(
        'userid',
        split_3col_2_dsl('line').alias('3col'),
        split_3col_2_dsl('line')['name'].alias('name'),
        split_3col_2_dsl('line')['address'].alias('address'),
        split_3col_2_dsl('line')['age'].alias('age')
    ).show()
