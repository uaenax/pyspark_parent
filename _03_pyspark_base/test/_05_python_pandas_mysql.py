import pandas as pd
from sqlalchemy import create_engine
if __name__ == '__main__':
    print("演示数据库的读写操作")
    df = pd.read_csv('/export/data/workspace/pyspark_parent/_03_pyspark_base/data/tsv示例文件.tsv', sep='\t', index_col=[0])
    # 构建链接数据库的对象:
    conn = create_engine('mysql+pymysql://root:123456@node1:3306/pyspark?charset=utf8')
    # 将df的数据写入到数据库中:
    # df.to_sql('test_pdtosql', conn, index=False, if_exists='append')

    # 从数据库中读取数据
    df = pd.read_sql('test_pdtosql', conn)
    print(df)

    df = pd.read_sql('select birthday from test_pdtosql', conn)
    print(df)