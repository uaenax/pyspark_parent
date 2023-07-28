from pyspark import SparkContext, SparkConf
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("累加器测试")
    conf = SparkConf().setMaster('local[*]').setAppName('accumulator')
    sc = SparkContext(conf=conf)
    # 定义一个变量,引入累加器
    a = sc.accumulator(10)
    # 读取数据
    rdd_init = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])


    # 处理数据:为a将列表中变量的值累加上去
    def fn1(num):
        # 对累加器进行增加
        a.add(num)
        return num


    rdd_map = rdd_init.map(fn1)
    rdd_map.cache().count()
    rdd_map2 = rdd_map.map(lambda a: a + 1)
    print(rdd_map.collect())
    print(rdd_map2.collect())
    # 获取累加器的结果
    print(a.value)
