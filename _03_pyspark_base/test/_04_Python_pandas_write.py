import pandas as pd
if __name__ == '__main__':
    print("通过pandas实现数据写出操作:csv")
    df = pd.read_csv('/export/data/workspace/pyspark_parent/_03_pyspark_base/data/1960-2019全球GDP数据.csv', encoding='gbk')
    df = df[df['country'] == '中国']
    df.to_csv('/export/data/workspace/pyspark_parent/_03_pyspark_base/data/china.csv', index=False, header=False, sep='|')
