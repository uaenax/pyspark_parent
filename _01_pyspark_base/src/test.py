from pyspark import SparkContext, SparkConf
import os

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('测试')
    sc = SparkContext(conf=conf)
    rdd_init = sc.textFile('file:///export/data/workspace/pyspark_parent/_01_pyspark_base/data/words.txt', 3)
    rdd_flatMap = rdd_init.flatMap(lambda word: word.split(' '))
    print(rdd_flatMap.getNumPartitions())
    print(rdd_flatMap.glom().collect())
    rdd_flatMap = rdd_flatMap.repartition(5)
    print(rdd_flatMap.getNumPartitions())
    print(rdd_flatMap.glom().collect())
    rdd_map = rdd_flatMap.map(lambda word: (word, 1))
    rdd_reduceByKey = rdd_map.foldByKey(10, lambda a, b: (a + b))
    print(rdd_reduceByKey.collect())
