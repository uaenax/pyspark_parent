from pyspark import SparkContext, SparkConf
import os

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('wd')
    sc = SparkContext(conf=conf)
    rdd_init = sc.wholeTextFiles('file:///export/data/workspace/pyspark_parent/_02_pyspark_base/data')
    rdd_init.saveAsTextFile('hdfs://node1:8020/pyspark_data/output')
    print(rdd_init.getNumPartitions())
    print(rdd_init.glom().collect())
