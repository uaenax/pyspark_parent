# 演示: pySpark入门案例:  WordCount
# 需求: 从HDFS中读取数据, 对数据进行统计分析(WordCount). 最后将结果根据单词数量进行倒序排序, 并将结果写出HDFS上
from pyspark import SparkContext, SparkConf
import os

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('wd')
    sc = SparkContext(conf=conf)
    rdd_init = sc.textFile('file:///export/data/workspace/pyspark_parent/_01_pyspark_base/data/data.txt')
    rdd_flatMap = rdd_init.flatMap(lambda line: line.split())
    rdd_map = rdd_flatMap.map(lambda word: (word.split('-')[1], 1))
    rdd_res = rdd_map.reduceByKey(lambda agg, curr: (agg + curr))
    print(rdd_res.collect())
    sc.stop()
