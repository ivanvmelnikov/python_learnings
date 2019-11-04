import pandas as pd

df1 = pd.DataFrame({'cA': ['A0', 'A1', 'A2'],
                    'cB': ['B0', 'B1', 'B2'],
                    'cC': ['C0', 'C1', 'C2']},
                   index=[0, 1, 2])

df2 = pd.DataFrame({'cA': ['A0', 'A1', 'A2'],
                    'cB': ['B2', 'B3', 'B4'],
                    'cC': ['C0', 'C1', 'C2']},
                   index=[0, 1, 2])

print(pd.concat([df1, df2]))
'''
cA  cB  cC
0  A0  B0  C0
1  A1  B1  C1
2  A2  B2  C2
0  A0  B2  C0
1  A1  B3  C1
2  A2  B4  C2
'''

merge = df1.merge(df2, how='left', indicator=True, left_on='cB', right_on='cB')

print(merge)
'''
  cA_x  cB cC_x cA_y cC_y     _merge
0   A0  B0   C0  NaN  NaN  left_only
1   A1  B1   C1  NaN  NaN  left_only
2   A2  B2   C2   A0   C0       both
'''

'''
return rows where column value doesn't exist in another dataframe
select * from df1 where df1.cB not int (select cB from df2)
 '''
print(df1[~df1['cB'].isin(df2['cB'])])
'''
   cA  cB  cC
0  A0  B0  C0
1  A1  B1  C1

'''
# remove column
print(df1.drop(columns=['cA']))
'''
   cB  cC
0  B0  C0
1  B1  C1
2  B2  C2
'''

# same
print(df1.drop(['cA'], axis=1))

# remove rows by index
print(df1.drop([0, 1]))
'''
   cA  cB  cC
2  A2  B2  C2
'''

# set value for column
df1c = df1.copy()
df1c['cA'] = 777
print(df1c)
'''
    cA  cB  cC
0  777  B0  C0
1  777  B1  C1
2  777  B2  C2
'''

# replace by regex
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.replace.html


print('get row by condition')
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.loc.html
tmp1 = df1.loc[df1['cA'] == 'A2']
print(tmp1)

print('same get row by condition?')
tmp2 = df1[df1['cA'] == 'A2']
print(tmp2)

print('get row  column value by condition')
tmp3 = df1.loc[df1['cA'] == 'A2']  # DataFrame
tmp4 = tmp3.iloc[0]  # Series
b_ = tmp4['cB']  # str
print(b_)

# combine column
fd1_copy = df1.copy()
fd1_copy['combined'] = fd1_copy['cA'].astype(str) + '_' + fd1_copy['cB']
print(fd1_copy)

'''
   cA  cB  cC combined
0  A0  B0  C0    A0_B0
1  A1  B1  C1    A1_B1
2  A2  B2  C2    A2_B2
'''
