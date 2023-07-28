from pyspark import SparkContext, SparkConf
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示广播变量的使用操作")
    # 创建SparkContext对象
    conf = SparkConf().setMaster('local[*]').setAppName('sougou')
    sc = SparkContext(conf=conf)

    # 设置广播变量
    bc = sc.broadcast(1000)
    # 读取数据
    rdd_init = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    # 将每个数据都加上指定值,此值由广播变量给出:
    # 获取广播: bc.value
    rdd_res = rdd_init.map(lambda num: num + bc.value)
    rdd_res.foreach(lambda num: print(num))
