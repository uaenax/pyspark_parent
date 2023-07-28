import pandas as pd

if __name__ == '__main__':
    print("pySpark模板")

    df = pd.read_csv('/export/data/workspace/pyspark_parent/_03_pyspark_base/data/1960-2019全球GDP数据.csv', encoding='gbk')
    china_gdp = df[df.country == '中国']
    print(china_gdp.head(10))
