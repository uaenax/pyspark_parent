from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


def TOP50():
    # 需求一: 找到TOP50热点题对应科目. 然后统计这些科目中, 分别包含几道热点题目
    # TOP50热点题
    df_top50 = spark.sql("""
            select 
                question_id,
                count(score) as cnt
            from t1
            group by question_id order by cnt desc limit 50
        """)
    df_top50.createTempView('t5_2')
    # 找到对应题目,以科目进行分组,统计热点题数量
    spark.sql("""
        select 
            t1.subject_id,
            count(distinct t5_2.question_id) as cnt_p
        from t5_2 join t1 on t5_2.question_id = t1.question_id 
        group by t1.subject_id 
    """).show()
    df_init.groupby('question_id').agg(
        F.count('score').alias('cnt')
    ).orderBy('cnt', ascending=False).limit(50).join(df_init, 'question_id').groupby('subject_id').agg(
        F.countDistinct(df_init['question_id']).alias('cnt_p')
    ).show()


def TOP20():
    # 需求二:  找到Top20热点题对应的推荐题目. 然后找到推荐题目对应的科目, 并统计每个科目分别包含推荐题目的条数
    # TOP20热点题
    df_top20 = spark.sql("""
                select
                    question_id,
                    count(score) as cnt
                from t1
                group by question_id order by cnt desc limit 20
            """)
    df_top20.createTempView('t2')
    # 对应的推荐题目
    df_topic = spark.sql("""
        select
            explode(split(t1.recommendations, ',')) as question_id
        from t2 join t1 on t2.question_id = t1.question_id
    """)
    df_topic.createTempView('t3')
    # 对应的科目, 并统计每个科目分别包含推荐题目的条数
    spark.sql("""
        select
            t1.subject_id ,
            count(distinct t3.question_id) as cnt
        from t3 join t1 on t3.question_id = t1.question_id
        group by t1.subject_id order by cnt desc
    """).show()
    df_top20_dsl = df_init.groupby('question_id').agg(
        F.count('score').alias('cnt')
    ).orderBy('cnt', ascending=False).limit(20)
    df_topic_dsl = df_top20_dsl.join(df_init, 'question_id').select(
        F.explode(F.split('recommendations', ',')).alias('question_id')
    )
    df_topic_dsl.join(df_init, 'question_id').groupby('subject_id').agg(
        F.countDistinct(df_topic_dsl['question_id']).alias('cnt')
    ).orderBy('cnt', ascending=False).show()


if __name__ == '__main__':
    print("在线教育案例需求实现")
    spark = SparkSession.builder.master('local[*]').appName('edu_01').getOrCreate()
    df_init = spark.read.csv(
        path='file:///export/data/workspace/pyspark_parent/_05_edu_project/data/eduxxx.csv',
        header=True,
        sep='\t',
        inferSchema=True
    )
    df_init.createTempView('t1')
    TOP50()

    TOP20()
