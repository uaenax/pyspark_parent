import pandas as pd

if __name__ == '__main__':
    print("pySpark模板")

    # 默认使用自增索引
    s2 = pd.Series([1, 2, 3])
    print(s2)
    # 自定义索引
    s3 = pd.Series([1, 2, 3], index=['a', 'b', 'c'])
    print(s3)
    # 使用元组
    tst = (1, 2, 3, 4, 5, 6)
    print(pd.Series(tst))
