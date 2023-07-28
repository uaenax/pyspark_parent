from pyspark import SparkContext, SparkConf
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("pySpark模板")
    conf = SparkConf().setMaster('local[*]').setAppName('自定义集合')
    sc = SparkContext(conf=conf)
    # rdd_init = sc.parallelize(['asdca', 'cascac', 'gfvea', 'veagfsav', 'bndfbvwgw'])
    # rdd_sort = rdd_init.sortBy(lambda word: word[0])
    # print(rdd_sort.collect())
    rdd_init = sc.textFile('hdfs://node1:8020/data/log.txt')
    rdd_map = rdd_init.map(lambda line: line.split())
    rdd_res = rdd_map.map(lambda line: (line[3].split(':')[2], 1))
    rdd_count = rdd_res.reduceByKey(lambda a, b: a + b)
    print(rdd_count.collect())
