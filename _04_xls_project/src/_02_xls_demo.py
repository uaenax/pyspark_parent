from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


def xuqiu1():
    # 需求1.客户数最多的10个国家
    spark.sql("""
        select 
            Country,
            count(distinct CustomerID) cnt_id
        from t1
        group by Country order by cnt_id desc limit 10
    """).show()
    df_init.groupby('Country').agg(
        F.countDistinct('CustomerID').alias('cnt_id')
    ).orderBy('cnt_id', ascending=False).limit(10).show()


def xuqiu2():
    # 需求2.销售量最多的10个国家
    spark.sql("""
            select 
                Country,
                sum( Quantity) qu_sum
            from t1
            group by Country order by qu_sum desc limit 10
        """).show()
    df_init.groupby('Country').agg(
        F.sum('Quantity').alias('qu_sum')
    ).orderBy('qu_sum', ascending=False).limit(10).show()


def xuqiu3():
    # 需求3.各个国家的总销售额分布情况
    spark.sql("""
        select 
            Country,
            round(sum(Quantity * UnitPrice), 2) as total_price
        from t1 where InvoiceNo not like 'C%'
        group by Country order by total_price desc 
    """).show()
    df_init.where('InvoiceNo not like "C%"').groupby('Country').agg(
        F.round(F.sum(df_init['Quantity'] * df_init['UnitPrice']), 2).alias('total_price')
    ).orderBy('total_price', ascending=False).show()


def xuqiu4():
    # 需求4.销量最高的10个商品
    spark.sql("""
        select 
            StockCode,
            sum(Quantity) sales
        from t1
        group by StockCode order by sales desc limit 10
    """).show()
    df_init.groupby('StockCode').agg(
        F.sum('Quantity').alias('sales')
    ).orderBy('sales', ascending=False).limit(10).show()


def xuqiu5():
    # 需求5.商品描述的热门关键词top300
    spark.sql("""
        select 
            words,
            count(1) as cnt_words
        from t1 lateral view explode (split(Description, ' ')) t2 as words
        group by words order by cnt_words desc limit 300
    """).show(300)
    df_init.withColumn('words', F.explode(F.split('Description', ' '))).groupby('words').agg(
        F.count('words').alias('cnt_words')
    ).orderBy('cnt_words', ascending=False).limit(300).show(300)


def xuqiu6():
    # 需求6.退货订单数最多的10个国家
    spark.sql("""
            select 
                Country,
                count(distinct InvoiceNo) cnt_order
            from t1 where InvoiceNo like 'C%'
            group by Country order by cnt_order desc limit 10
        """).show()
    df_init.where('InvoiceNo like "C%"').groupby('Country').agg(
        F.countDistinct('InvoiceNo').alias('cnt_order')
    ).orderBy('cnt_order', ascending=False).limit(10).show()


def xuqiu7():
    # 需求7.月销售额随时间的变化趋势
    spark.sql("""
        select 
            month(InvoiceDate) as month,
            round(sum(Quantity * UnitPrice), 2) as total_price
        from t1 where InvoiceNo not like 'C%'
        group by month(InvoiceDate)  order by total_price desc 
    """).show()
    df_init.where("InvoiceNo not like 'C%'").groupby(F.month('InvoiceDate')).agg(
        F.round(F.sum(df_init['Quantity'] * df_init['UnitPrice']), 2).alias('total_price')
    ).orderBy('total_price', ascending=False).show()


def xuqiu8():
    # 需求8.日销售额随时间的变化趋势
    spark.sql("""
            select 
                day(InvoiceDate) as month,
                round(sum(Quantity * UnitPrice), 2) as total_price
            from t1 where InvoiceNo not like 'C%'
            group by day(InvoiceDate)  order by total_price desc 
        """).show()
    df_init.where("InvoiceNo not like 'C%'").groupby(F.dayofmonth('InvoiceDate')).agg(
        F.round(F.sum(df_init['Quantity'] * df_init['UnitPrice']), 2).alias('total_price')
    ).orderBy('total_price', ascending=False).show()


def xuqiu9():
    # 需求9.各国的购买订单量和退货订单量的关系
    spark.sql("""
        select 
            Country,
            count(distinct InvoiceNo)  as cnt_oid,
            count(distinct if (InvoiceNo like 'C%', InvoiceNo, null)) as c_cnt_oid
        from t1
        group by Country order by cnt_oid desc 
    """).show()
    df_init.groupby('Country').agg(
        F.countDistinct('InvoiceNo').alias('cnt_oid'),
        F.countDistinct(F.expr('if (InvoiceNo like "C%", InvoiceNo, null)')).alias('c_cnt_oid')
    ).orderBy('cnt_oid', ascending=False).show()


def xuqiu10():
    # 需求10.商品的平均单价与销量的关系
    spark.sql("""
        select 
            StockCode,
            avg(UnitPrice) avg_price,
            sum(Quantity) sum_num
        from t1
        group by StockCode order by sum_num desc 
    """).show()
    df_init.groupby('StockCode').agg(
        F.avg('UnitPrice').alias('avg_price'),
        F.sum('Quantity').alias('sum_num')
    ).orderBy('sum_num', ascending=False).show()


if __name__ == '__main__':
    print("完成新零售案例的需求实现操作")
    spark = SparkSession.builder.master('local[*]').appName('xls_demo').getOrCreate()
    schema = StructType() \
        .add('InvoiceNo', StringType()) \
        .add('StockCode', StringType()) \
        .add('Description', StringType()) \
        .add('Quantity', IntegerType()) \
        .add('InvoiceDate', StringType()) \
        .add('UnitPrice', DoubleType()) \
        .add('CustomerID', IntegerType()) \
        .add('Country', StringType())
    df_init = spark.read.csv(
        path='hdfs://node1:8020/xls/output',
        sep='\001',
        schema=schema
    )
    df_init.createTempView('t1')

    xuqiu1()

    xuqiu2()

    xuqiu3()

    xuqiu4()

    xuqiu5()

    xuqiu6()

    xuqiu7()

    xuqiu8()

    xuqiu9()

    xuqiu10()
