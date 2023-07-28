from pyspark import SparkContext, SparkConf
import os

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("构建RDD")
    conf = SparkConf().setMaster('local[*]').setAppName('createRDD')
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize(['张三', '李四', '王五', '赵六', '田七', '周八'], 4)
    print(rdd_init.getNumPartitions())
    print(rdd_init.glom().collect())
