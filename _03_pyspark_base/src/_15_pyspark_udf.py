from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示如何自定义UDF函数")
    spark = SparkSession.builder.master('local[*]').appName('udf_01').config("spark.sql.shuffle.partitions",
                                                                               "4").getOrCreate()
    df_init = spark.read.csv(path='file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/student.csv',
                             schema='id int,name string,age int')
    df_init.createTempView('t1')


    # 处理数据
    # 需求:自定义函数需求 请在name名称后面添加一个 _itcast
    # 自定义一个Python函数,完成主题功能
    # 方式三:不能和方式二共用
    @F.udf(returnType=StringType())
    def concat_udf(name):
        return name + '_itcast'


    # 将函数注册个spark sql
    # # 方式一
    # conat_udf_dsl = spark.udf.register('concat_udf_sql', concat_udf, StringType())
    # # 方式二
    # concat_udf_dsl_2 = F.udf(concat_udf, StringType())

    # 使用自定义函数
    # SQL使用
    # spark.sql("""
    #     select
    #     id,concat_udf_sql(name) as name, age
    #     from t1
    # """).show()
    # DSL使用
    # df_init.select(
    #     df_init['id'],
    #     conat_udf_dsl(df_init['name']).alias('name'),
    #     df_init['age']
    # ).show()
    #
    # df_init.select(
    #     df_init['id'],
    #     concat_udf_dsl_2(df_init['name']).alias('name'),
    #     df_init['age']
    # ).show()
    df_init.select(
        df_init['id'],
        concat_udf(df_init['name']).alias('name'),
        df_init['age']
    ).show()
