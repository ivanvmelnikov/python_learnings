import importlib
import datetime

PRODUCT_ID = 'product_id'

B_2_C_PRODUCT_TRANCHE = 'b2c_product_tranche'

B_2_C_INTEREST_PRODUCT = 'b2c_interest_product'

B_2_C_PRODUCT_BANK = 'b2c_product_bank'

INTEREST_PRODUCT_ID = 'interestProduct_id'

PRODUCT_BANK_ID = 'productBank_id'

ID = 'id'

now = datetime.datetime.utcnow()

from pandas_examples import pandas_mysql_example as ms

PRODUCT_IDENTIFIER = 'productIdentifier'
BIC = 'bic'

PASSWORD = 'mysqlroot'
PORT = '3316'
from_db_name = 'comonea_b2c_prod'
to_db_name = 'comonea_b2c'


def remove_id_column(to):
    return to.drop(columns=[ID])


def get_column_values_list(column_name, df):
    return [r[column_name] for index, r in df.iterrows()]


def set_current_creation_date(to):
    to['creationDate'] = now.strftime('%Y-%m-%d %H:%M:%S')


def update_foreign_key(df, foreign_key_c_name, ref_table_secondary_key_name, ref_table_f, ref_table_t):
    for index, r in df.iterrows():
        secondary_key_val = ref_table_f[ref_table_f[ID] == r[foreign_key_c_name]].iloc[0][ref_table_secondary_key_name]
        new_foreign_key_val = ref_table_t[ref_table_t[ref_table_secondary_key_name] == secondary_key_val].iloc[0][ID]
        print("%s %s -> %s" % (foreign_key_c_name, r[foreign_key_c_name], new_foreign_key_val))
        df.loc[index, foreign_key_c_name] = new_foreign_key_val


pt_f = ms.load_mysql_df(from_db_name, B_2_C_PRODUCT_TRANCHE, password=PASSWORD, port=PORT)
pt_t = ms.load_mysql_df(to_db_name, B_2_C_PRODUCT_TRANCHE, password=PASSWORD, port=PORT)

tranches_not_in_to = pt_f[~pt_f[PRODUCT_IDENTIFIER].isin(pt_t[PRODUCT_IDENTIFIER])].copy()

pb_f = ms.load_mysql_df(from_db_name, B_2_C_PRODUCT_BANK, password=PASSWORD, port=PORT)
pb_t = ms.load_mysql_df(to_db_name, B_2_C_PRODUCT_BANK, password=PASSWORD, port=PORT)
pbs_not_in_to = pb_f[~pb_f[BIC].isin(pb_t[BIC])]

set_current_creation_date(pbs_not_in_to)
# insert product missing product banks
ms.insert_mysql_df(to_db_name, B_2_C_PRODUCT_BANK, remove_id_column(pbs_not_in_to),
                   password=PASSWORD, port=PORT, commit=True)

new_pb_t = ms.load_mysql_df(to_db_name, B_2_C_PRODUCT_BANK, password=PASSWORD, port=PORT)

ip_f = ms.load_mysql_df(from_db_name, B_2_C_INTEREST_PRODUCT, password=PASSWORD, port=PORT)
ip_t = ms.load_mysql_df(to_db_name, B_2_C_INTEREST_PRODUCT, password=PASSWORD, port=PORT)
ips_not_in_to = ip_f[~ip_f[PRODUCT_IDENTIFIER].isin(ip_t[PRODUCT_IDENTIFIER])].copy()

print("pb_id before ")
print(ips_not_in_to.head(1)[PRODUCT_BANK_ID])

update_foreign_key(ips_not_in_to, PRODUCT_BANK_ID, BIC, pb_f, new_pb_t)

print("pb_id after ")
print(ips_not_in_to.head(1)[PRODUCT_BANK_ID])

set_current_creation_date(ips_not_in_to)

ms.insert_mysql_df(to_db_name, B_2_C_INTEREST_PRODUCT, remove_id_column(ips_not_in_to),
                   password=PASSWORD, port=PORT, commit=True)

new_ip_t = ms.load_mysql_df(to_db_name, B_2_C_INTEREST_PRODUCT, password=PASSWORD, port=PORT)

print("ip_id before ")
print(tranches_not_in_to.head(1)[INTEREST_PRODUCT_ID])

print("ip_id for tranche:")
update_foreign_key(tranches_not_in_to, INTEREST_PRODUCT_ID, PRODUCT_IDENTIFIER, ip_f, new_ip_t)

print("ip_id after ")
print(tranches_not_in_to.head(1)[INTEREST_PRODUCT_ID])

set_current_creation_date(tranches_not_in_to)

ms.insert_mysql_df(to_db_name, B_2_C_PRODUCT_TRANCHE, remove_id_column(tranches_not_in_to),
                   password=PASSWORD, port=PORT, commit=True)
new_pt_t = ms.load_mysql_df(to_db_name, B_2_C_PRODUCT_TRANCHE, password=PASSWORD, port=PORT)

pdia_f = ms.load_mysql_df(from_db_name, 'b2c_product_bank_interest_account', password=PASSWORD, port=PORT)
pdia_t = ms.load_mysql_df(to_db_name, 'b2c_product_bank_interest_account', password=PASSWORD, port=PORT)

pdia_f_diff = pdia_f[pdia_f[PRODUCT_ID].isin(tranches_not_in_to[ID])].copy()
'''
"b2c_product_bank_interest_account"
"serviceBankTransitAccount_id"->"b2c_service_bank_transit_account.id"
"productBank_id"->"b2c_product_bank.id"
"product_id"->"b2c_product_tranche.id"
"currency"->"b2c_currency.currency_code"
'''
pdia_f_diff['serviceBankTransitAccount_id'] = 7
update_foreign_key(pdia_f_diff, PRODUCT_BANK_ID, BIC, pb_f, new_pb_t)
update_foreign_key(pdia_f_diff, PRODUCT_ID, PRODUCT_IDENTIFIER, pt_f, new_pt_t)
set_current_creation_date(pdia_f_diff)

print('inserting %s b2c_product_bank_interest_account'% pdia_f_diff.shape[0])
ms.insert_mysql_df(to_db_name, "b2c_product_bank_interest_account", remove_id_column(pdia_f_diff),
                   password=PASSWORD, port=PORT, commit=True)

ir_f = ms.load_mysql_df(from_db_name, 'b2c_interest_rate', password=PASSWORD, port=PORT)

ir_f_diff = ir_f[ir_f[PRODUCT_ID].isin(tranches_not_in_to[ID])].copy()

print('new_pt_id for interest rate:')
update_foreign_key(ir_f_diff, PRODUCT_ID, PRODUCT_IDENTIFIER, pt_f, new_pt_t)

set_current_creation_date(ir_f_diff)

ms.insert_mysql_df(to_db_name, 'b2c_interest_rate', remove_id_column(ir_f_diff),
                   password=PASSWORD, port=PORT, commit=True)
