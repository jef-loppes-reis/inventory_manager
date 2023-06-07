import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from tqdm import tqdm
from datetime import date, datetime
from requests import put
from math import floor
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from itertools import repeat
from time import sleep, perf_counter

from pecista import Postgres, MLInterface


def convert_temp(seconds):
    seconds = seconds % (24 * 3600) 
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    return "%d:%02d:%02d" % (hour, minutes, seconds) 

def todoprodutos_today_name() -> str:
    """Função para retornar string.

    Returns
    -------
    str
        todosprodutos-2023_02_16
    """
    data_atual = date.today().strftime('%Y_%m_%d')
    data_atual = 'todosProdutos-' + data_atual

    return data_atual

def condicao_if(
        preco_kaizen:float, cost_frete:float) -> float:
    """Função para calcular o preço final do produto.

    Parameters
    ----------
    preco_kaizen : float
        P_KAIZEN

    cost_frete : float
        Custo de frete item_id

    Returns
    -------
    float
        Calculo da função
    """
    #----- Antiga Regra -----
    # if cost_frete < 17.95:
    #     cost_frete = 17.95
    # if preco_kaizen + cost_frete >= 79.00:
    #     return (1, round(preco_kaizen + cost_frete, 2))
    # elif (preco_kaizen + cost_frete < 79.00) and (preco_kaizen + (2*cost_frete) >= 79.00):
    #     return (2, 79.00)
    # elif (preco_kaizen + (2*cost_frete) < 79.00):
    #     return (3, round(preco_kaizen + 6.03, 2))
    if preco_kaizen < 7:
        preco_kaizen = 7
    if preco_kaizen >= 79.00:
        return (1, round(preco_kaizen + cost_frete, 2))
    if (preco_kaizen + 6.63 >= 79.00) & (preco_kaizen < 79.00):
        return (2, 78.99)
    return (3, preco_kaizen + 6.63)

def upgrade_price_stock(
        mlb:str,
        variation_id:int,
        price:float,
        stock:int,
        access_token:str='',
        condition_price:bool=False,
        condition_price_storage:bool=False,
        condition_storage:bool=False) -> tuple:

    url = f"https://api.mercadolibre.com/items/{mlb}"

    if condition_price_storage:
        # Preço e Estoque
        payload_price_storage = {
            'variations':[
                {
                'id': int(variation_id),
                'price': round(price,2),
                'available_quantity': int(stock)
                }
            ]
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
            }
        response = put(url=url, json=payload_price_storage, headers=headers)
        return response.status_code, response.text

    if condition_storage:
        # Estoque
        payload_storage = {
            'variations':[
                {
                'id': int(variation_id),
                'available_quantity': int(stock)
                }
            ]
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
            }

        response = put(url=url, json=payload_storage, headers=headers)
        return response.status_code, response.text

    if condition_price:
        # Preco
        payload_price = {
            'variations':[
                {
                'id': int(variation_id),
                'price': round(price,2)
                }
            ]
        }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
            }

        response = put(url=url, json=payload_price, headers=headers)
        return response.status_code, response.text
    
def interation(mlb_:str, variation_id_:int, price_:float, quantity_:int, idx:int, access_token:str='') -> dict:

    response, response_text = upgrade_price_stock(
        mlb=mlb_,
        variation_id=variation_id_,
        price=price_,
        stock=quantity_,
        access_token=access_token,
        condition_price_storage=True
        )

    if response == 400:
        response, response_text = upgrade_price_stock(
            mlb=mlb_,
            variation_id=variation_id_,
            price=price_,
            stock=quantity_,
            access_token=access_token,
            condition_price_storage=True
        )
    # if response == 500:
    #     # print(mlb_, response)
    #     pass
    # if response == 401:
    #     # print('Token desatualizado !')
    #     pass

    while (response == 429) or (response == 500):
        sleep(10)
        # print('Aguardando requisição !',mlb_, response)
        response, response_text = upgrade_price_stock(
            mlb=mlb_,
            variation_id=variation_id_,
            price=price_,
            stock=quantity_,
            access_token=access_token,
            condition_price_storage=True
            )

    return {'item_id':mlb_, 'price':price_, 'storage':quantity_, 'response':response, 'response_text':response_text, 'idx':idx}

def update_bank(df_aux:pd.DataFrame, conta_1:bool=False):

    table = 'ml1_info' if conta_1 else 'ml2_info'
    with Postgres() as db: db.update(df=df_aux, id_col='item_id', table=table)

def data_bases(conta:bool):

    with Postgres() as db:
        if conta:
            df_ml_info = db.query('SELECT * FROM "ECOMM".ml1_info')
            df_ml_frete = db.query('SELECT * FROM "ECOMM".ml1_frete')
        else:
            df_ml_info = db.query('SELECT * FROM "ECOMM".ml2_info')
            df_ml_frete = db.query('SELECT * FROM "ECOMM".ml2_frete')

        df_siac_storage = db.query('''
            SELECT tb_produto.codpro, tb_produto.num_fab as sku, tb_produto.embala, tb_estoque.estoque
            FROM "D-1".produto as tb_produto
            INNER JOIN (SELECT codpro, SUM(estoque) as ESTOQUE FROM "H-1".prd_loja GROUP BY codpro) as tb_estoque
            ON tb_produto.codpro = tb_estoque.codpro''').fillna({'estoque':0})
        
        df_price_kaizen = pd.read_csv(f'./data/{todoprodutos_today_name()}.csv', sep=';', low_memory=False, dtype=str)[['NRFABRICA', 'P_KAIZEN']]

        return df_ml_info, df_ml_frete, df_siac_storage, df_price_kaizen
    


if __name__ == '__main__':

    print(f'\nAtualizando VARIAVES...')
    start_temp = perf_counter()

    ml = MLInterface()
    ACCESS_TOKEN = ml._get_token(1)
    CONTA_1 = True

    df_ml_info, df_ml_frete, df_siac_storage, df_price_kaizen = data_bases(CONTA_1)
    df_siac_storage['embala'] = df_siac_storage.embala.replace(0,1)
    df_variation = df_ml_info.query('variation and ~kit and ~exception').reset_index(drop=True)

    df_price_kaizen['NRFABRICA'] = df_price_kaizen.NRFABRICA.str.replace('[','').str.replace(']','')
    df_price_kaizen['P_KAIZEN'] = df_price_kaizen.P_KAIZEN.str.replace(',','.')
    df_price_kaizen = df_price_kaizen.astype({'P_KAIZEN':float})
    df_price_kaizen = df_price_kaizen.rename({'NRFABRICA':'sku', 'P_KAIZEN':'price_kaizen'}, axis=1)

    df_variation = pd.merge(df_variation, df_price_kaizen, on='sku', how='left')
    df_variation = pd.merge(df_variation, df_ml_frete, on='item_id', how='left')
    df_variation = pd.merge(df_variation, df_siac_storage[['sku', 'estoque', 'embala']], on='sku', how='left')

    df_variation = df_variation.query('~embala.isna()').reset_index(drop=True)
    df_variation = df_variation.astype({'embala':int})

    df_variation['estoque_variation'] = (df_variation.estoque / df_variation.embala).apply(lambda x : floor(x))
    df_variation['price_variation'] = df_variation.embala * df_variation.price_kaizen

    for idx in tqdm(df_variation.index):
        price = df_variation.loc[idx, 'price_variation']
        list_cost = df_variation.loc[idx, 'list_cost']
        df_variation.loc[idx, 'price_variation'] = condicao_if(price, list_cost)[1]

    df_variation_out = df_variation[['item_id', 'estoque_variation', 'price_variation']]

    df_variation_out = pd.merge(df_ml_info, df_variation_out, on='item_id', how='left').query('variation and ~price_variation.isna()').reset_index(drop=True)

    for idx in tqdm(df_variation_out.index, total=len(df_variation_out)):
        df_variation_out.loc[idx, 'atualizar'] = (df_variation_out.loc[idx, 'price'] != df_variation_out.loc[idx, 'price_variation']) or (df_variation_out.loc[idx, 'storage'] != df_variation_out.loc[idx, 'estoque_variation'])

    # print(df_variation_out.query('item_id == "MLB1516572116" ')['price_variation'])
    df_aux = pd.DataFrame(columns=['item_id', 'price', 'storage', 'response', 'response_text'])
    with ProcessPoolExecutor() as exe:
        for future in tqdm(exe.map(
            interation,
            df_variation_out['item_id'],
            df_variation_out['variation_id'],
            df_variation_out['price_variation'],
            df_variation_out['estoque_variation'],
            df_variation_out.index,
            repeat(ACCESS_TOKEN)),
            total=len(df_variation_out)):
            # print(future['item_id'], future['response'], future['idx'])
            df_aux.loc[len(df_aux)] = future

    update_bank( df_aux.query('response == 200')[['item_id', 'price', 'storage']].reset_index(drop=True), conta_1=True )

    if len(df_aux.query('response != 200')) != 0: 
        df_aux.query('response != 200').reset_index(drop=True).to_csv(f'temp/variations/routine_variations_{str(datetime.today().strftime("%m%d%Y_%H%M%S"))}.csv', index=False)

    end_temp = perf_counter()

    string_log = f'''
### updated variations ###
{datetime.today().strftime('%m/%d/%Y, %H:%M:%S')}
Quantidade de items_ids = {len(df_aux['item_id'])}
Quantidade de itens atualizados = {len(df_aux.query('response == 200'))}
Quantidade de itens não atualizados = {len(df_aux['item_id']) - len(df_aux.query('response == 200'))}
Tempo total de execução {convert_temp(seconds=int(end_temp-start_temp))}'''

    print(string_log)