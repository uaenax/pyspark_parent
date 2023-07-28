import jieba
from pyspark import SparkContext, SparkConf, StorageLevel
import os
import time

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


def xuqiu_1():
    # 获取关键词
    rdd_map_search = rdd_map.map(lambda line_tup: line_tup[2])
    # 对搜索词进行分词操作
    rdd_keywords = rdd_map_search.flatMap(lambda search: jieba.cut(search))
    # 将每个关键词转换为(关键词,1)进行分组统计
    rdd_res = rdd_keywords.map(lambda keyword: (keyword, 1)).reduceByKey(lambda agg, curr: agg + curr)
    # 对结果数据进行倒序排序
    rdd_sort = rdd_res.sortBy(lambda res: res[1], ascending=False)
    # 获取结果(前50)
    print(rdd_sort.take(50))


def xuqiu_2():
    # 提取 用户和搜索词数据
    rdd_user_search = rdd_map.map(lambda line_tup: (line_tup[1], line_tup[2]))
    # 基于用户和搜索词进行分组统计即可
    rdd_res = rdd_user_search.map(lambda line: (line, 1)).reduceByKey(lambda agg, curr: agg + curr)
    rdd_sort = rdd_res.sortBy(lambda res: res[1], ascending=False)
    print(rdd_sort.take(30))


def xuqiu_3():
    # 获取时间(每个小时)
    rdd_time = rdd_map.map(lambda time: (time[0].split(':')[0], 1)).reduceByKey(lambda a, b: a + b)
    print(rdd_time.collect())


if __name__ == '__main__':
    print("搜狗案例")
    # 创建SparkContext对象
    conf = SparkConf().setMaster('local[*]').setAppName('sougou')
    sc = SparkContext(conf=conf)

    # 设置检查点位置
    sc.setCheckpointDir('/spark/checkpoint/')
    # 读取外部文件数据
    rdd_init = sc.textFile('file:///export/data/workspace/pyspark_parent/_02_pyspark_base/data/SogouQ.sample')
    # 过滤数据:保证数据不能为空 并且数据字段数量必须为6个
    rdd_flatmap = rdd_init.filter(lambda line: line.strip() != '' and len(line.split()) == 6)
    # 对数据进行切割,将数据放置到一个元组中,一行放置一个元组
    rdd_map = rdd_flatmap.map(lambda line: (
        line.split()[0],
        line.split()[1],
        line.split()[2][1:-1],
        line.split()[3],
        line.split()[4],
        line.split()[5],
    ))
    # 设置缓存的代码
    # rdd_map.persist(storageLevel=StorageLevel.MEMORY_AND_DISK).count()

    # 开启检查点
    rdd_map.checkpoint()
    rdd_map.count()

    xuqiu_1()

    # 手动清理缓存
    # rdd_map.unpersist().count()

    xuqiu_2()

    xuqiu_3()

    time.sleep(1000)
