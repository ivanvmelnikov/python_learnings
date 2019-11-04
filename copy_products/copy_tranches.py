import importlib
import datetime

T_F_PAIR = 'f_t_pair'

TARGET_INTEREST_ACCOUNT_ID = 'targetProductBankInterestAccount_id'

SOURCE_INTEREST_ACCOUNT_ID = 'sourceProductBankInterestAccount_id'

PRODUCT_TRANCHE_T = 'product_tranche_t'

PRODUCT_TRANCHE_F = 'product_tranche_f'

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
PASSWORD_TO = 'mysqlroot'
PORT = '3316'
PORT_TO = '3316'
USER_NAME_TO = 'root'
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
pt_t = ms.load_mysql_df(to_db_name, B_2_C_PRODUCT_TRANCHE, password=PASSWORD_TO, port=PORT_TO, user_name=USER_NAME_TO)

tranches_not_in_to = pt_f[~pt_f[PRODUCT_IDENTIFIER].isin(pt_t[PRODUCT_IDENTIFIER])].copy()

pb_f = ms.load_mysql_df(from_db_name, B_2_C_PRODUCT_BANK, password=PASSWORD, port=PORT)
pb_t = ms.load_mysql_df(to_db_name, B_2_C_PRODUCT_BANK, password=PASSWORD_TO, port=PORT_TO, user_name=USER_NAME_TO)
pbs_not_in_to = pb_f[~pb_f[BIC].isin(pb_t[BIC])]

set_current_creation_date(pbs_not_in_to)
# insert product missing product banks
ms.insert_mysql_df(to_db_name, B_2_C_PRODUCT_BANK, remove_id_column(pbs_not_in_to),
                   password=PASSWORD_TO, port=PORT_TO, user_name=USER_NAME_TO, commit=True)

new_pb_t = ms.load_mysql_df(to_db_name, B_2_C_PRODUCT_BANK, password=PASSWORD_TO, port=PORT_TO, user_name=USER_NAME_TO)

ip_f = ms.load_mysql_df(from_db_name, B_2_C_INTEREST_PRODUCT, password=PASSWORD, port=PORT)
ip_t = ms.load_mysql_df(to_db_name, B_2_C_INTEREST_PRODUCT, password=PASSWORD_TO, port=PORT_TO, user_name=USER_NAME_TO)
ips_not_in_to = ip_f[~ip_f[PRODUCT_IDENTIFIER].isin(ip_t[PRODUCT_IDENTIFIER])].copy()

print("pb_id before ")
print(ips_not_in_to.head(1)[PRODUCT_BANK_ID])

update_foreign_key(ips_not_in_to, PRODUCT_BANK_ID, BIC, pb_f, new_pb_t)

print("pb_id after ")
print(ips_not_in_to.head(1)[PRODUCT_BANK_ID])

set_current_creation_date(ips_not_in_to)

ms.insert_mysql_df(to_db_name, B_2_C_INTEREST_PRODUCT, remove_id_column(ips_not_in_to),
                   password=PASSWORD_TO, port=PORT_TO, user_name=USER_NAME_TO, commit=True)

new_ip_t = ms.load_mysql_df(to_db_name, B_2_C_INTEREST_PRODUCT, password=PASSWORD_TO, port=PORT_TO,
                            user_name=USER_NAME_TO)

print("ip_id before ")
print(tranches_not_in_to.head(1)[INTEREST_PRODUCT_ID])

print("ip_id for tranche:")
update_foreign_key(tranches_not_in_to, INTEREST_PRODUCT_ID, PRODUCT_IDENTIFIER, ip_f, new_ip_t)

print("ip_id after ")
print(tranches_not_in_to.head(1)[INTEREST_PRODUCT_ID])

set_current_creation_date(tranches_not_in_to)

ms.insert_mysql_df(to_db_name, B_2_C_PRODUCT_TRANCHE, remove_id_column(tranches_not_in_to),
                   password=PASSWORD_TO, port=PORT_TO, user_name=USER_NAME_TO, commit=True)
new_pt_t = ms.load_mysql_df(to_db_name, B_2_C_PRODUCT_TRANCHE, password=PASSWORD_TO, port=PORT_TO,
                            user_name=USER_NAME_TO)

pdia_f = ms.load_mysql_df(from_db_name, 'b2c_product_bank_interest_account', password=PASSWORD, port=PORT)
pdia_t = ms.load_mysql_df(to_db_name, 'b2c_product_bank_interest_account', password=PASSWORD_TO, port=PORT_TO,
                          user_name=USER_NAME_TO)

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

print('inserting %s b2c_product_bank_interest_account' % pdia_f_diff.shape[0])
ms.insert_mysql_df(to_db_name, "b2c_product_bank_interest_account", remove_id_column(pdia_f_diff),
                   password=PASSWORD_TO, port=PORT_TO, user_name=USER_NAME_TO, commit=True)

new_pdia_t = ms.load_mysql_df(to_db_name, 'b2c_product_bank_interest_account', password=PASSWORD_TO, port=PORT_TO,
                              user_name=USER_NAME_TO)

ir_f = ms.load_mysql_df(from_db_name, 'b2c_interest_rate', password=PASSWORD, port=PORT)

ir_f_diff = ir_f[ir_f[PRODUCT_ID].isin(tranches_not_in_to[ID])].copy()

print('new_pt_id for interest rate:')
update_foreign_key(ir_f_diff, PRODUCT_ID, PRODUCT_IDENTIFIER, pt_f, new_pt_t)

set_current_creation_date(ir_f_diff)

ms.insert_mysql_df(to_db_name, 'b2c_interest_rate', remove_id_column(ir_f_diff),
                   password=PASSWORD_TO, port=PORT_TO, user_name=USER_NAME_TO, commit=True)

pbm_f = ms.load_mysql_df(from_db_name, 'b2c_service_bank_product_mapping', password=PASSWORD, port=PORT)

pbm_f_diff = pbm_f[pbm_f[PRODUCT_ID].isin(tranches_not_in_to[ID])].copy()

pbm_f_diff['serviceBank_id'] = 12
pbm_f_diff['transitAccount_id'] = 7
update_foreign_key(pbm_f_diff, PRODUCT_ID, PRODUCT_IDENTIFIER, pt_f, new_pt_t)
update_foreign_key(pbm_f_diff, 'interestAccount_id', 'uuid', pdia_f, new_pdia_t)
update_foreign_key(pbm_f_diff, 'transitAccount_id', 'uuid', pdia_f, new_pdia_t)

set_current_creation_date(pbm_f_diff)
ms.insert_mysql_df(to_db_name, 'b2c_service_bank_product_mapping', remove_id_column(pbm_f_diff),
                   password=PASSWORD_TO, port=PORT_TO, user_name=USER_NAME_TO, commit=True)

im_f = ms.load_mysql_df(from_db_name, 'b2c_product_change_investment_mapping', password=PASSWORD, port=PORT)
im_t = ms.load_mysql_df(to_db_name, 'b2c_product_change_investment_mapping', password=PASSWORD_TO, port=PORT_TO,
                        user_name=USER_NAME_TO)

im_f_copy = im_f.copy()
im_t_copy = im_t.copy()


def add_tranche_id(im_copy, pbia_foreign_key_c, pdia, pt, new_col):
    for index, r in im_copy.iterrows():
        product_tr_id = pdia[pdia[ID] == r[pbia_foreign_key_c]].iloc[0][PRODUCT_ID]
        product_tr_identifier = pt[pt[ID] == product_tr_id].iloc[0][PRODUCT_IDENTIFIER]
        im_copy.loc[index, new_col] = product_tr_identifier


add_tranche_id(im_t_copy, SOURCE_INTEREST_ACCOUNT_ID, new_pdia_t, new_pt_t, PRODUCT_TRANCHE_F)
add_tranche_id(im_t_copy, TARGET_INTEREST_ACCOUNT_ID, new_pdia_t, new_pt_t, PRODUCT_TRANCHE_T)
im_t_copy[T_F_PAIR] = im_t_copy[PRODUCT_TRANCHE_F].astype(str) + '_' + im_t_copy[PRODUCT_TRANCHE_T]

add_tranche_id(im_f_copy, SOURCE_INTEREST_ACCOUNT_ID, pdia_f, pt_f, PRODUCT_TRANCHE_F)
add_tranche_id(im_f_copy, TARGET_INTEREST_ACCOUNT_ID, pdia_f, pt_f, PRODUCT_TRANCHE_T)
im_f_copy[T_F_PAIR] = im_f_copy[PRODUCT_TRANCHE_F].astype(str) + '_' + im_f_copy[PRODUCT_TRANCHE_T]

im_f_diff = im_f_copy[~im_f_copy[T_F_PAIR].isin(im_t_copy[T_F_PAIR])].copy()


def replace_pbia_with_new_id(im, tmp_foreign_key_name, foreign_key_name):
    for index, r in im.iterrows():
        tranche_id = new_pt_t[new_pt_t[PRODUCT_IDENTIFIER] == r[tmp_foreign_key_name]].iloc[0][ID]
        pbia_id = new_pdia_t[new_pdia_t[PRODUCT_ID] == tranche_id].iloc[0][ID]
        im.loc[index, foreign_key_name] = pbia_id


replace_pbia_with_new_id(im_f_diff, PRODUCT_TRANCHE_F, SOURCE_INTEREST_ACCOUNT_ID)
replace_pbia_with_new_id(im_f_diff, PRODUCT_TRANCHE_T, TARGET_INTEREST_ACCOUNT_ID)

set_current_creation_date(im_f_diff)

im_to_insert = remove_id_column(im_f_diff).drop(columns=[PRODUCT_TRANCHE_F]).drop(columns=[PRODUCT_TRANCHE_T]).drop(
    columns=[T_F_PAIR])

print('Diff size' + str(im_f_diff.shape[0]))

ms.insert_mysql_df(to_db_name, 'b2c_product_change_investment_mapping', im_to_insert,
                   password=PASSWORD_TO, port=PORT_TO, user_name=USER_NAME_TO, commit=True)
