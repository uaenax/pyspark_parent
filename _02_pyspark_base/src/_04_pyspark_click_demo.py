from pyspark import SparkContext, SparkConf
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("点击流日志分析")
    conf = SparkConf().setMaster('local[*]').setAppName('sougou')
    sc = SparkContext(conf=conf)
    rdd_init = sc.textFile('file:///export/data/workspace/pyspark_parent/_02_pyspark_base/data/access.log')
    rdd_filter = rdd_init.filter(lambda line: line.strip() != '' and len(line.split()) > 12)
    print(f'pv:访问量: {rdd_filter.count()}')
    print(f'nv:独立客户访问量:{rdd_filter.map(lambda line: line.split()[0]).distinct().count()}')
    print(f'访问前十的URL:{rdd_filter.map(lambda line:(line.split()[6],1)).reduceByKey(lambda a, b:a + b).sortBy(lambda res: res[1],ascending=False).take(10)}')
