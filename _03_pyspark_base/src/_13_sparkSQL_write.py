from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示数据写出到文件中")
    spark = SparkSession.builder.master('local[*]').appName('cleanAPI').config("spark.sql.shuffle.partitions",
                                                                               "4").getOrCreate()
    df_init = spark.read.csv(path='file:///export/data/workspace/pyspark_parent/_03_pyspark_base/data/stu.csv',
                             sep=',',
                             header=True,
                             inferSchema=True
                             )
    # 去重API:df.dropDuplicates()
    df = df_init.dropDuplicates()
    # 删除null值数据: df.dropna()
    df = df_init.dropna()

    # # 将清洗后的数据写出到文件中
    # # 演示写出为csv文件
    # df.write.mode('overwrite').format('csv').option('header', True).option('sep', '|').save(
    #     'hdfs://node1:8020/sparkwrite/output1')
    # # 演示写出为json文件
    # df.write.mode('overwrite').format('json').save(
    #     'hdfs://node1:8020/sparkwrite/output2')
    # # 演示输出为text
    # df.select('name').write.mode('overwrite').format('text').save('hdfs://node1:8020/sparkwrite/output3')
    # # 演示输出为orc
    # df.write.mode('overwrite').format('orc').save('hdfs://node1:8020/sparkwrite/output4')
    df.write.mode('overwrite').format('jdbc') \
        .option('url', 'jdbc:mysql://node1:3306/pyspark?useSSL=false&useUnicode=true&characterEncoding=utf-8') \
        .option('dbtable', 'stu')\
        .option('user', 'root')\
        .option('password', '123456')\
        .save()
