from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


def xuqiu1():
    # 需求1.查询用户平均分
    df_init.createTempView('t1_1')
    spark.sql("""
        select userid,round(avg(score),2) as s_avg from t1_1 group by userid order by s_avg desc 
    """).show()
    df_init.groupby('userid').agg(
        F.round(F.avg('score'), 2).alias('s_avg')
    ).orderBy(F.desc('s_avg')).show()


def xuqiu2():
    # 需求2.查询电影平均分
    df_init.createTempView('t1_2')
    spark.sql("""
            select movie,round(avg(score),2) as s_avg from t1_2 group by movie  
        """).show()
    df_init.groupby('movie').agg(
        F.round(F.avg('score'), 2).alias('s_avg')
    ).orderBy(F.desc('s_avg')).show()


def xuqiu3():
    # 需求3.查询大于平均分的电影的数量
    df_init.createTempView('t1_3')
    spark.sql("""
        select count(distinct t1_3.movie) from t1_3 join (select movie,round(avg(score),2) as s_avg from t1_2 group by movie ) as t on t1_3.movie = t.movie
        where t1_3.score > t.s_avg
    """).show()
    df_avg = df_init.groupby('movie').agg(F.round(F.avg('score'), 2).alias('s_avg')).orderBy(F.desc('s_avg'))
    df_init.join(df_avg, 'movie').where(df_init['score'] > df_avg['s_avg']).agg(
        F.countDistinct('movie')
    ).show()


def xuqiu4():
    # 需求四:查询高分电影中(>3) 打分次数最多的用户,并求出此人打的平均分
    # 找出所有的高分电影
    df_init.createTempView('t1')
    df_top_movie = spark.sql("""
        select movie,round(avg(score), 2) as avg_score from t1 group by movie having avg_score > 3
    """)
    df_top_movie.createTempView('t2')
    # 在高分电影中找到打分次数最多的用户
    df_u_top = spark.sql("""
        select t1.userid from t2 join t1 on t2.movie = t1.movie group by userid order by count(1) desc limit 1
    """)
    df_u_top.createTempView('t3')
    # 并求出此人在所有的电影中打的平均分
    spark.sql("""
        select userid,round(avg(score), 2) as avg_score from t1 where userid = (select userid from t3) group by userid
    """).show()
    # 找出所有的高分电影
    df_top_movie = df_init.groupby('movie').agg(F.round(F.avg('score'), 2).alias('avg_score')).where('avg_score > 3')
    # 在高分电影中找到打分次数最多的用户
    df_u_top = df_top_movie.join(df_init, 'movie').groupby('userid').agg(F.count('userid').alias('u_cnt')).orderBy(
        F.desc('u_cnt')).select('userid').limit(1)
    # 并求出此人在所有的电影中打的平均分
    # df_u_top.first()['userid']:df_u_top是一个二维表 调用first获取第一行的数据,['字段名']获取对应列的数据的值
    df_init.where(df_init['userid'] == df_u_top.first()['userid']).groupby('userid').agg(
        F.round(F.avg('score'), 2).alias('avg_score')
    ).show()


def xuqiu5():
    # 需求五 查询每个用户的平均打分,最低打分,最高打分
    df_init.createTempView('t5_1')
    spark.sql("""
        select userid,round(avg(score), 2) as u_avg,min(score) as u_min,max(score) as u_max from t5_1 group by userid 
    """).show()
    df_init.groupby('userid').agg(
        F.round(F.avg('score'), 2).alias('u_avg'), F.min('score').alias('u_min'), F.max('score').alias('u_max')
    ).show()


def xuqiu6():
    # 需求六 查询评分超过一百次的电影的平均分 排名top10
    df_init.createTempView('t6')
    spark.sql("""
        select movie,round(avg(score), 2) as cnt from t6 group by movie having count(1) > 100 order by cnt desc limit 10 
    """).show()
    df_init.groupby('movie').agg(F.round(F.avg('score'), 2).alias('cnt')).where(F.count('score') > 100).orderBy(
        F.desc('cnt')).show(10)


if __name__ == '__main__':
    print("演示电影分析案例")
    # 创建SparkSession对象:
    spark = SparkSession.builder.master('local[*]').appName('movie').getOrCreate()
    # 读取hdfs上movie数据集
    df_init = spark.read.csv(path='hdfs://node1:8020/spark/movie_data/u.data', sep='\t',
                             schema='userid string,movie string,score int,datestr string')
    xuqiu1()

    xuqiu2()

    xuqiu3()

    xuqiu4()

    xuqiu5()

    xuqiu6()