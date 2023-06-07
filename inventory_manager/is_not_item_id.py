import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from requests import get
from tqdm import tqdm
from time import perf_counter
from json import load
from os import system
from time import sleep

from pecista import Postgres


def convert_temp(seconds): 
    seconds = seconds % (24 * 3600) 
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    return "%d:%02d:%02d" % (hour, minutes, seconds)

def parse_args():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('--conta', help='Token de acesso a API ML', required=True, type=int)
    args = parser.parse_args()
    
    return args

# Criando lita de excecoes
def mlb_exceptions_list():
    with open('./data/products/mlb_exception_conta_1.json') as json_data:
        data = load(json_data)
        list_exception_conta_1 = [
            *data['FULL'],
            *data['PALHETAS']
        ]

    list_exception_conta_2 = []

    return list_exception_conta_1, list_exception_conta_2

#========= Base de Dados ===============

# Baixando todos MLBs do MercadoLivre
def get_inf_mlbs(conta:int):
    if conta == 1:
        log_args = 'py ./inventory_manager/get_infos_mlbs.py --out ./data/products/results_get_itens_conta_1.csv --conta 1'
        system(log_args)
    else:
        log_args = 'py ./inventory_manager/get_infos_mlbs.py --out ./data/products/results_get_itens_conta_2.csv --conta 2'
        system(log_args)

def import_dataframes(conta:int):

    if conta == 1:
        df_realtrends = pd.read_csv('./data/products/results_get_itens_conta_1.csv', dtype=str)
    else:
        df_realtrends = pd.read_csv('./data/products/results_get_itens_conta_2.csv', dtype=str)

    df_realtrends = df_realtrends.query('~item_id.duplicated()').reset_index(drop=True)
    df_realtrends = df_realtrends.query('~status.str.startswith("under_review")').reset_index(drop=True)

    df_realtrends['item_sku'] = df_realtrends.item_sku.str.upper()
    df_realtrends['variation_sku'] = df_realtrends.variation_sku.str.upper()

    with Postgres() as db:
        df_produtos_siac = db.query('select codpro, num_fab from "D-1".produto')

    return df_realtrends, df_produtos_siac

# Analise e tratamento dos Dados importados
def analitic(df_realtrends:pd.DataFrame, df_produtos_siac:pd.DataFrame, list_exception_conta_1:list, list_exception_conta_2:list, conta:int):

    for idx in df_realtrends.index:
        variation_sku = df_realtrends.loc[idx, 'variation_sku']
        item_sku = df_realtrends.loc[idx, 'item_sku']

        if pd.isna(item_sku) and ~pd.isna(variation_sku):
            df_realtrends.loc[idx, 'sku'] = variation_sku
        else:
            df_realtrends.loc[idx, 'sku'] = item_sku
    
    if len(df_realtrends.item_id) == 0:
        return df_realtrends
    else:
        df_realtrends['jogo'] = df_realtrends.sku.str.startswith('J_')
        df_realtrends['kit'] = df_realtrends.sku.str.startswith('K_')
        df_realtrends['storage'] = df_realtrends['available_quantity'].astype(int)
        df_realtrends['exception'] = df_realtrends.item_id.isin(list_exception_conta_1) if conta == 1 else df_realtrends.item_id.isin(list_exception_conta_2)
        df_realtrends['variation'] = ~df_realtrends.variation_id.isna()
        df_realtrends = pd.merge(df_realtrends, df_produtos_siac.rename({'num_fab':'sku'}, axis=1), on='sku', how='left')
        # df_realtrends = df_realtrends.query('~codpro.isna()')
        return df_realtrends
    
#========= Criando DataFrame MLB_FRETE =========
def call_tables_db(conta:int):
    with Postgres() as db:
        if conta == 1:
            # df_ml1 = db.query('select * from "ECOMM".ml1_info')
            df_ml1_frete = db.query('select * from "ECOMM".ml1_frete')
            return df_ml1_frete
        else:
            # df_ml2 = db.query('select * from "ECOMM".ml2_info')
            df_ml2_frete = db.query('select * from "ECOMM".ml2_frete')
            return df_ml2_frete
        
def get_shipping_cost(item_id:str):
    url = f"https://api.mercadolibre.com/items/{item_id}/shipping_options/free"
    try:
        response = get(url=url)
        # print(response.status_code)
        if response.status_code == 400:
            response = get(url=url)

        while (response.status_code == 429) or (response.status_code == 500):
            sleep(10)
            response = get(url=url)

        return response.json()['coverage']['all_country']['list_cost'], item_id
    except:
        return 0, item_id
    
def run_get_shipping_cost_id(df_ml_frete:pd.DataFrame):

    with ThreadPoolExecutor() as exe:
        for future in tqdm(exe.map(get_shipping_cost, df_ml_frete.item_id), total=len(df_ml_frete.item_id)):
            index = df_ml_frete.query(f'item_id == "{future[1]}"').index[0]
            df_ml_frete.loc[index, 'list_cost'] = future[0]

    return df_ml_frete

#========= INSERTE/UPDATE dos Dados no Postgres ==========
def ins_or_upd_inf_ml_frete(conta:int, insert:False, update:False, df:pd.DataFrame):
    if conta == 1:
        table = 'ml1_frete'
    else:
        table = 'ml2_frete'

    if insert:
        with Postgres() as db:
            db.insert(
                df,
                table
            )
    if update:
        with Postgres() as db:
            db.update(
                df,
                'item_id',
                table
            )

def insert_ml_info(conta:int, df:pd.DataFrame=pd.DataFrame()):
    
    if conta == 1:
        table = 'ml1_info'
    else:
        table = 'ml2_info'
    with Postgres() as db:
        db.insert(
            df[['item_id', 'price', 'storage', 'codpro', 'sku', 'jogo', 'kit', 'exception', 'variation', 'variation_id', 'category_id', 'date_created']],
            table
        )

# Funcao TRUNCATE no Postgres
def truncate(conta:int):
    if conta == 1:
        with Postgres() as db:
            db.query('truncate table "ECOMM".ml1_frete')
            db.query('truncate table "ECOMM".ml1_info')
    else:
        with Postgres() as db:
            db.query('truncate table "ECOMM".ml2_frete')
            db.query('truncate table "ECOMM".ml2_info')


# MAIN
if __name__ == '__main__':
    
    args = parse_args()
    CONTA_ARGS = args.conta

    start_temp = perf_counter()
    print('Carregando dataframes...')
    get_inf_mlbs(conta=CONTA_ARGS)
    list_exception_conta_1, list_exception_conta_2 = mlb_exceptions_list()
    df_realtrends, df_produtos_siac = import_dataframes(conta=CONTA_ARGS)
    df_comp = analitic(df_realtrends=df_realtrends, df_produtos_siac=df_produtos_siac, list_exception_conta_1=list_exception_conta_1, list_exception_conta_2=list_exception_conta_2,conta=CONTA_ARGS)
    df_comp = df_comp.query('~sku.isna()')

    print('\nAplicando funcao Truncate no Postgres...')
    truncate(conta=CONTA_ARGS)

    print('Inserindo informacoes do Dataframe ao Postgres...')
    if len(df_comp) != 0:
        print(f'\n{len(df_comp.item_id)} itens para inserir ao banco.')
        insert_ml_info(conta=CONTA_ARGS, df=df_comp)
    else:
        print("\nNao a nada para atualizar\n")

    print('')
    print('Requisitando informacoes de frete...')
    ins_or_upd_inf_ml_frete(conta=CONTA_ARGS, insert=True, update=False, df=df_comp[['item_id']] )

    df_ml_frete = call_tables_db(conta=CONTA_ARGS)
    df_ml_frete = run_get_shipping_cost_id(df_ml_frete=df_ml_frete)

    ins_or_upd_inf_ml_frete(conta=CONTA_ARGS, insert=False, update=True, df=df_ml_frete)

    end_temp = perf_counter()

    print(f'Codigo finalizado, tempo corrido de {convert_temp(end_temp - start_temp)}\n')