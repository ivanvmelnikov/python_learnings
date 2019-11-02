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
