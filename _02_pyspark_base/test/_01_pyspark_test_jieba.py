from pyspark import SparkContext, SparkConf
import os
import jieba

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print(list(jieba.cut('我毕业于清华大学')))
    print(list(jieba.cut('我毕业于清华大学', cut_all=True)))
    print(list(jieba.cut_for_searh('我毕业于清华大学')))