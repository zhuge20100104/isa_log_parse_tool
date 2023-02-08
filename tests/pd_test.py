import pandas as pd

dic1 = {"A": [1, 1, 2, 3], 'B': [2,4,5,6]}
dic2 = {"A": [1, 1, 2, 2, 3], 'B': [2, 3, 4, 5, 6]}

df1 = pd.DataFrame(dic1)
df2 = pd.DataFrame(dic2)

df1 = df1.drop_duplicates('A',keep='last')
df2 = df2.drop_duplicates('A',keep='last')
res_df = pd.merge(df1, df2, on=["A"],  how="inner")

print(df1.head(1000))
print(df2.head(1000))
print(res_df.head(1000))
