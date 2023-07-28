# 演示: pySpark入门案例:  WordCount
# 需求: 从HDFS中读取数据, 对数据进行统计分析(WordCount). 最后将结果根据单词数量进行倒序排序, 并将结果写出HDFS上
from pyspark import SparkContext, SparkConf
import os

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("WordCount案例:从hdfs读取数据")
    conf = SparkConf().setAppName('wd')
    sc = SparkContext(conf=conf)
    rdd_init = sc.textFile('hdfs://node1:8020/pyspark_data/words.txt')
    rdd_flatMap = rdd_init.flatMap(lambda line: line.split())
    rdd_map = rdd_flatMap.map(lambda word: (word, 1))
    rdd_res = rdd_map.reduceByKey(lambda agg, curr: (agg + curr))
    rdd_sort = rdd_res.sortBy(lambda wd_tup: wd_tup[1])
    # rdd_sort.saveAsTextFile('hdfs://node1:8020/pyspark_data/output')
    print(rdd_sort.collect())
    sc.stop()
