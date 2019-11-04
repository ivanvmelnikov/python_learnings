from sqlalchemy import create_engine

import pandas as pd


def load_mysql_df(db_name, table_name, user_name='root', password='123456', host='127.0.0.1', port='3306'):
    # dialect[+driver]://user:password@host/dbname[?key=value..]``
    sqlEngine = create_mysql_engine(db_name, host, password, port, user_name)
    dbConnection = sqlEngine.connect()
    frame = pd.read_sql(f'select * from {table_name}', dbConnection)
    pd.set_option('display.expand_frame_repr', False)
    dbConnection.close()
    return frame


def create_mysql_engine(db_name, host, password, port, user_name):
    sqlEngine = create_engine(f'mysql+pymysql://{user_name}:{password}@{host}:{port}/{db_name}', pool_recycle=3600)
    return sqlEngine


# print(load_mysql_df('comonea_b2c', 'b2c_product_bank', password='mysqlroot', port='3316'))


def insert_mysql_df(db_name, table_name, df, user_name='root', password='123456', host='127.0.0.1', port='3306',
                    commit=False):
    print('inserting into %s amount of row: %s' % (table_name, df.shape[0]))
    # dialect[+driver]://user:password@host/dbname[?key=value..]``
    sqlEngine = create_mysql_engine(db_name, host, password, port, user_name)
    dbConnection = sqlEngine.connect()
    tx = dbConnection.begin()
    try:
        sql = df.to_sql(table_name, con=dbConnection, if_exists='append', chunksize=1000, index=False)
        # print(sql)
        if commit:
            tx.commit()
        else:
            tx.rollback()
    except:
        tx.rollback()
        raise
    finally:
        dbConnection.close()
