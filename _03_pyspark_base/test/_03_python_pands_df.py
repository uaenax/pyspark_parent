import pandas as pd

if __name__ == '__main__':
    print("pandas的dataFrame相关内容")

    # 构建dataframe对象:字典方式
    df = pd.DataFrame(data={
        'name': ['张三', '李四', '王五'],
        'age': [20, 25, 28],
        'address': ['北京', '上海', '深圳']
    })
    print(df)

    # 通过列表的方式
    df = pd.DataFrame(data=[['张三', 20, '北京'], ['李四', 25, '上海'], ['王五', 28, '深圳']], columns=['name', 'age', 'address'],
                      index=['a', 'b', 'c'])
    print(df)

    # 通过列表+元组
    df = pd.DataFrame(data=[('张三', 20, '北京'), ('李四', 25, '上海'), ('王五', 28, '深圳')], columns=['name', 'age', 'address'],
                      index=['a', 'b', 'c'])
    print(df)

    # 通过元组
    df = pd.DataFrame(data=(('张三', 20, '北京'), ('李四', 25, '上海'), ('王五', 28, '深圳')), columns=['name', 'age', 'address'],
                      index=['a', 'b', 'c'])
    print(df)

    # 常见API
    df = pd.DataFrame(data=(('张三', 20, '北京'), ('李四', 25, '上海'), ('王五', 28, '深圳')), columns=['name', 'age', 'address'],
                      index=['a', 'b', 'c'])
    print(len(df))
    print(df.size)

    for col_name in df:
        for val in df[col_name]:
            print(val)

    # 执行一些相关操作
    df1 = pd.DataFrame(data=(('张三', 20, '北京'), ('李四', 25, '上海'), ('王五', 28, '深圳')), columns=['name', 'age', 'address'],
                       index=['a', 'b', 'c'])
    df2 = pd.DataFrame(data=(('张三', 20, '北京'), ('李四', 25, '上海'), ('王五', 28, '深圳')), columns=['name', 'age', 'address'],
                       index=['a', 'b', 'c'])
    print(df1 + df2)
    print(df1['age'] * df2['age'])
