from pyspark import SparkContext, SparkConf
import os

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print('pyspark的入门案例:WordCount')

    # 1.创建spark核心对象:SparkContext
    conf = SparkConf().setAppName('wordCount')
    sc = SparkContext(conf=conf)

    # 2.读取数据
    rdd_init = sc.textFile('file:///export/data/workspace/pyspark_parent/_01_pyspark_base/data/words.txt')

    # 3.对数据进行扁平化处理
    rdd_flatmap = rdd_init.flatMap(lambda line: line.split(' '))

    # 4.将每一个单词转换为(单词,1)
    rdd_map = rdd_flatmap.map(lambda word: (word, 1))

    # 5.根据key进行分组聚合统计操作
    rdd_res = rdd_map.reduceByKey(lambda agg, curr: agg+curr)
    print(rdd_res.collect())
