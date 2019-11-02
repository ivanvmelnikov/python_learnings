from sqlalchemy import create_engine

import pandas as pd


def load_mysql_df(db_name, table_name, user_name='root', password='123456', host='127.0.0.1', port='3306'):
    # dialect[+driver]://user:password@host/dbname[?key=value..]``
    sqlEngine = create_engine(f'mysql+pymysql://{user_name}:{password}@{host}:{port}', pool_recycle=3600)
    dbConnection = sqlEngine.connect()
    frame = pd.read_sql(f'select * from {db_name}.{table_name}', dbConnection)
    pd.set_option('display.expand_frame_repr', False)
    dbConnection.close()
    return frame

# print(load_mysql_df('comonea_b2c','b2c_product_bank', password='mysqlroot', port='3316'))
