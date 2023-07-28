from pyspark import SparkContext, SparkConf
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("combinerByKey演示")
    conf = SparkConf().setMaster('local[2]').setAppName('combinerByKey')
    sc = SparkContext(conf=conf)
    rdd_init = sc.parallelize(
        [('c01', '张三'), ('c02', '李四'), ('c01', '王五'), ('c03', '赵六'), ('c02', '田七'), ('c03', '周八'), ('c01', '李九'),
         ('c02', '老张'), ('c01', '老李')])
    # 需求:
    """
        要求将数据转换为以下格式: 
            [
                ('c01',['张三','王五','李九','老李']),
                ('c02',['李四','田七','老张']),
                ('c03',['赵六','周八'])
            ]
    """


    def fn1(agg):
        return [agg]


    def fn2(agg, curr):
        agg.append(curr)
        return agg


    def fn3(agg, curr):
        # print(agg)
        agg.extend(curr)
        return agg


    rdd_res = rdd_init.combineByKey(fn1, fn2, fn3)
    print(rdd_res.collect())
