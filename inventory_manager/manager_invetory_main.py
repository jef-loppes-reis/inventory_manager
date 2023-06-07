from concurrent.futures import ProcessPoolExecutor # Modulo para PY
from tqdm import tqdm
from pecista import Postgres, MLInterface
from datetime import date, datetime
from math import floor
from time import perf_counter
from requests import put
from time import sleep
from itertools import repeat
from pywhatkit import sendwhatmsg_instantly as msg, sendwhatmsg_to_group_instantly as msg_group
from os import path, listdir
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


#                                        =-=-=-=-=-=-=-=-=-=-=-= Variaveis constante =-=-=-=-=-=-=-=-=-=-=-=

PATH_OUT = './out'
PATH_DATA = './data/'
PATH_TEMP = './temp'

QUERY_PRODUTO = 'SELECT codpro, produto, codfor, codgru, num_fab, num_orig, embala, fantasia, in_lixeira FROM "D-1".produto'
QUERY_ESTOQUE = 'SELECT cd_loja, codpro, estoque FROM "H-1".prd_loja'
QUERY_ML1_FRETE = 'SELECT * FROM "ECOMM".ml1_frete'
QUERY_ML2_FRETE = 'SELECT * FROM "ECOMM".ml2_frete'
QUERY_ML1_INFO = 'SELECT * FROM "ECOMM".ml1_info'
QUERY_ML2_INFO = 'SELECT * FROM "ECOMM".ml2_info'

DATA_HORA_ATUAL = str(datetime.today())

#                                             =-=-=-=-=-=-=-=-=-=-=-= Funções =-=-=-=-=-=-=-=-=-=-=-=

#-> Ferramentas

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

def todos_produtos_recente(path_:str) -> str:
    temp_max = 0
    arquivo_hoje = ''
    for arquivo in listdir(path_):
        temp = path.getmtime(path_+arquivo)
        if temp > temp_max:
            temp_max = temp
            arquivo_hoje = arquivo
    return arquivo_hoje

#-> Codigo
def divide_stock_by_mutiple(
        df_merge_produtos_estoque_preco_mlbs:pd.DataFrame) -> list:
    """Função para dividir o estoque pelo multiplo do produto.

    Parameters
    ----------
    df_merge_produtos_estoque_preco_mlbs : pd.DataFrame
        df_merge_produtos_estoque_preco_mlbs

    Returns
    -------
    list
        list_divide
    """
    list_divide = []

    for idx in df_merge_produtos_estoque_preco_mlbs.index:

        result = floor(df_merge_produtos_estoque_preco_mlbs.loc[idx, 'estoque'] / df_merge_produtos_estoque_preco_mlbs.loc[idx, 'embala'])

        list_divide.append(result)

    return list_divide

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

def read_tables_postgres(conta_1:bool=False) -> tuple:
    """Função para importar tabelas do postegres e dataframes terceiros.

    Returns
    -------
    tuple
        df_produtos_postgres; df_estoqueProdutos_postgres; df_item_id_cost_frete_ml1; df_item_id_cost_frete_ml2
    """

    # df_produtos_postgres = db.query(QUERY_PRODUTO)
    with Postgres() as db: df_produtos_postgres = db.query(QUERY_PRODUTO)

    # df_estoqueProdutos_postgres = db.query(QUERY_ESTOQUE)
    with Postgres() as db: df_estoqueProdutos_postgres = db.query(QUERY_ESTOQUE)

    if conta_1:
        with Postgres() as db: df_item_id_cost_frete_ml = db.query(QUERY_ML1_FRETE)
        return df_produtos_postgres, df_estoqueProdutos_postgres, df_item_id_cost_frete_ml
    else:
        with Postgres() as db: df_item_id_cost_frete_ml = db.query(QUERY_ML1_FRETE)
        return df_produtos_postgres, df_estoqueProdutos_postgres, df_item_id_cost_frete_ml

def read_tables_excel(conta_1:bool=False) -> tuple:
    """Função para carregar as planilhas, TodosProdutos, Lista de MLBs e Custos de Fretes de cada MLB

    Returns
    -------
    tuple
        df_todosprodutos, df_mlbs_skus
    """
    try:
        df_todosprodutos = pd.read_csv(f'{PATH_DATA}{todoprodutos_today_name()}.csv', sep=';', low_memory=False, dtype='str')[
            ['CODIGO',
            'NRFABRICA',
            'ORIGINAL',
            'P_KAIZEN']
            ].rename(
            {'CODIGO': 'codpro',
            'NRFABRICA': 'num_fab',
            'ORIGINAL': 'num_orig'}, axis=1)
    except Exception as error:
        print(f'Erro ! todosprodutos.xlsx\n{str(error)}')

    if conta_1:
        try:
            df_mlbs_skus = pd.read_csv(f'{PATH_DATA}/products/results_get_itens_conta_1.csv')[
                ['item_id',
                'item_sku',
                'variation_id',
                'variation_sku',
                'title',
                'available_quantity'
                ]
            ].rename({'item_sku':'num_fab'}, axis=1)
        except Exception as error:
            print(f'Erro ! dataframe com a lista de todos MLBs\n{error}')
    else:
        try:
            df_mlbs_skus = pd.read_csv(f'{PATH_DATA}/products/results_get_itens_conta_2.csv')[
                ['item_id',
                'item_sku',
                'variation_id',
                'variation_sku',
                'title',
                'available_quantity'
                ]
            ].rename({'item_sku':'num_fab'}, axis=1)
        except Exception as error:
            print(f'Erro ! dataframe com a lista de todos MLBs\n{error}')

    return df_todosprodutos, df_mlbs_skus

def merge_products_estoque(
        df_estoqueProdutos_postgres:pd.DataFrame,
        df_produtos_postgres:pd.DataFrame) -> pd.DataFrame:
    """Faz uma mesclagem de dois dataframes.

    Parameters
    ----------
    df_estoqueProdutos_postgres : pd.DataFrame
        Dataframe CODPRO; CD_LOJA; ESTOQUE; ...
    
    df_produtos_postgres : pd.DataFrame
        Dataframe CODPRO; NUM_FAB; NUM_ORIG; ...

    Returns
    -------
    pd.DataFrame
        df_merge_produtos_estoque = CODPRO; NUM_FAB; ESTOQUE; ...
    """
    df_estoqueProdutos_postgres = df_estoqueProdutos_postgres.groupby('codpro').agg({'estoque':sum}).reset_index()

    df_merge_produtos_estoque = pd.merge(
        df_produtos_postgres, df_estoqueProdutos_postgres, left_on='codpro', right_on='codpro', how='left'
    ).fillna({'estoque':0})

    return df_merge_produtos_estoque

def merge_products_estoque_price(
        df_merge_produtos_estoque:pd.DataFrame,
        df_todosprodutos:pd.DataFrame) -> pd.DataFrame:
    """Mescla dois dataframes.

    Parameters
    ----------
    df_merge_produtos_estoque : pd.DataFrame
        Dataframe CODPRO; NUM_FAB; ESTOQUE; ...
    df_todosprodutos : pd.DataFrame
        Dataframe CODIGO; NUFABRICA; P_KAIZEN; ...

    Returns
    -------
    pd.DataFrame
        df_merge_produtos_estoque_preco = CODPRO; NUM_FAB; ESTOQUE; P_KAIZEN; ...
    """
    df_merge_produtos_estoque_preco = pd.merge(
        df_merge_produtos_estoque, df_todosprodutos[['codpro', 'P_KAIZEN']], left_on='codpro', right_on='codpro', how='left'
        ).fillna({'P_KAIZEN':0})

    return df_merge_produtos_estoque_preco

def merge_products_estoque_price_mlbs(
        df_merge_produtos_estoque_preco:pd.DataFrame,
        df_mlbs_skus:pd.DataFrame) -> pd.DataFrame:
    """Mescla dois dataframes, e faz alguns tratamentos.

    Parameters
    ----------
    df_merge_produtos_estoque_preco : pd.DataFrame
        _description_
    df_mlbs_skus : pd.DataFrame
        _description_

    Returns
    -------
    pd.DataFrame
        df_merge_produtos_estoque_preco_mlbs
    """
    df_merge_produtos_estoque_preco_mlbs = pd.merge(
        df_merge_produtos_estoque_preco, df_mlbs_skus, left_on='num_fab', right_on='num_fab', how='left').fillna(
        {
        'available_quantity':0,
        'item_id':'NA','estoque':0,
        'embala':1})

    # Tratando a coluna embala, subistituindo o multiplo 0 por 1
    df_merge_produtos_estoque_preco_mlbs.embala = df_merge_produtos_estoque_preco_mlbs.embala.replace(0, 1)

    # Filtrando somente itens existentes no ML
    # df_merge_produtos_estoque_preco_mlbs = df_merge_produtos_estoque_preco_mlbs[ df_merge_produtos_estoque_preco_mlbs['price'] != 'NA'].reset_index(drop=True)

    # Convertendo tipo de dado das colunas
    df_merge_produtos_estoque_preco_mlbs.P_KAIZEN = df_merge_produtos_estoque_preco_mlbs.P_KAIZEN.str.replace(',','.').astype(float)
    df_merge_produtos_estoque_preco_mlbs.estoque = df_merge_produtos_estoque_preco_mlbs.estoque.astype(int)
    df_merge_produtos_estoque_preco_mlbs.available_quantity = df_merge_produtos_estoque_preco_mlbs.available_quantity.astype(int)

    # Multiplicando PREÇO KAIZEN * embala (MULTIPLO)
    df_merge_produtos_estoque_preco_mlbs.P_KAIZEN = round(df_merge_produtos_estoque_preco_mlbs.embala * df_merge_produtos_estoque_preco_mlbs.P_KAIZEN, 2).fillna({'P_KAIZEN':0})

    df_merge_produtos_estoque_preco_mlbs.estoque = divide_stock_by_mutiple(df_merge_produtos_estoque_preco_mlbs)

    df_merge_produtos_estoque_preco_mlbs.estoque = df_merge_produtos_estoque_preco_mlbs.estoque.apply( lambda x : 0 if x < 0 else x)

    return df_merge_produtos_estoque_preco_mlbs

def merge_pkStockMlbs_mlbsCostFrete(
        df_merge_produtos_estoque_preco_mlbs:pd.DataFrame,
        df_item_id_cost_frete_ml1_or_ml2:pd.DataFrame) -> pd.DataFrame:


    df_merge_pkStockMlbs_mlbsCostFrete = pd.merge(
        df_merge_produtos_estoque_preco_mlbs, df_item_id_cost_frete_ml1_or_ml2, left_on='item_id', right_on='item_id', how='left'
    ).fillna({'list_cost':0, 'P_KAIZEN':0}).reset_index(drop=True)

    for idx in df_merge_pkStockMlbs_mlbsCostFrete.index:

        p_kaizen = df_merge_pkStockMlbs_mlbsCostFrete.loc[idx, 'P_KAIZEN']
        cost_shipping = df_merge_pkStockMlbs_mlbsCostFrete.loc[idx, 'list_cost']

        df_merge_pkStockMlbs_mlbsCostFrete.loc[idx, 'calc_price_ml'] = condicao_if(preco_kaizen=p_kaizen, cost_frete=cost_shipping)[1]

    return df_merge_pkStockMlbs_mlbsCostFrete

def call_table_ml_infos(
        conta_1:bool=False
    ) -> pd.DataFrame:
    """SELECT das tabelas postgres ECOMMERCE

    Parameters
    ----------
    conta1 : bool, optional
        _description_, by default False

    conta2 : bool, optional
        _description_, by default False

    Returns
    -------
    pd.DataFrame
        _description_
    """
    if conta_1:
        with Postgres() as db: df_banco = db.query(QUERY_ML1_INFO)
        return df_banco
    else:
        with Postgres() as db: df_banco = db.query(QUERY_ML2_INFO)
        return df_banco

def comp(df_merge_pkStockMlbs_mlbsCostFrete:pd.DataFrame, atualizar_todos_prod=False, storage_only:bool=False, conta_1:bool=False) -> pd.DataFrame:
    """Faz comparação do dataframe atual com o dataframe antigo.

    Parameters
    ----------
    df_merge_pkStockMlbs_mlbsCostFrete : pd.DataFrame
        _description_

    Returns
    -------
    pd.DataFrame
        df_filtro_1
    """
    df = call_table_ml_infos(conta_1=conta_1)
    df = df.query("~ variation and ~ jogo and ~ exception")

    df_merge_comp = pd.merge(
        df_merge_pkStockMlbs_mlbsCostFrete[['item_id', 'price', 'storage']], df[['item_id', 'variation', 'price', 'storage']], on='item_id', how='inner'
    )

    if storage_only:
        for idx in df_merge_comp.index:
            df_merge_comp.loc[idx, 'atualizar'] = df_merge_comp.loc[idx, 'storage_x'] != df_merge_comp.loc[idx, 'storage_y']
    else:
        for idx in df_merge_comp.index:
            df_merge_comp.loc[idx, 'atualizar'] = (df_merge_comp.loc[idx, 'storage_x'] != df_merge_comp.loc[idx, 'storage_y']) or (df_merge_comp.loc[idx, 'price_x'] != df_merge_comp.loc[idx, 'price_y'])

    if atualizar_todos_prod:
        return df_merge_pkStockMlbs_mlbsCostFrete[['item_id', 'price', 'storage']]

    # Filtro somente com os items_ids que precisam ser atualizados
    df_filtro_1 = df_merge_pkStockMlbs_mlbsCostFrete[ df_merge_pkStockMlbs_mlbsCostFrete['item_id'].isin(df_merge_comp.query('atualizar')['item_id']) ].reset_index(drop=True)
    return df_filtro_1[['item_id', 'price', 'storage']]

def update_bank(df_aux:pd.DataFrame, conta_1:bool=False):

    table = 'ml1_info' if conta_1 else 'ml2_info'
    with Postgres() as db: db.update(df=df_aux, id_col='item_id', table=table)

#                                        =-=-=-=-=-=-=-=-=-=-=-= Funções para atualizar =-=-=-=-=-=-=-=-=-=-=-=

def upgrade_price_stock(
        mlb:str,
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
            'price': float(price),
            'available_quantity': int(stock)
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
            'available_quantity': int(stock)
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
            'price': float(price)
            }
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
            }

        response = put(url=url, json=payload_price, headers=headers)
        return response.status_code, response.text

def interation(mlb_:str, price_:float, quantity_:int, idx:int, access_token:str='') -> dict:

    response, response_text = upgrade_price_stock(
        mlb=mlb_,
        price=price_,
        stock=quantity_,
        access_token=access_token,
        condition_price_storage=True,
        condition_storage=False
        )

    if response == 400:
        response, response_text = upgrade_price_stock(
            mlb=mlb_,
            price=price_,
            stock=quantity_,
            access_token=access_token,
            condition_price_storage=True,
            condition_storage=False
        )

    while (response == 429) or (response == 500):
        # print('Aguardando requisição !',mlb_, response)
        sleep(10)
        response, response_text = upgrade_price_stock(
            mlb=mlb_,
            price=price_,
            stock=quantity_,
            access_token=access_token,
            condition_price_storage=True,
            condition_storage=False
            )

    return {'item_id':mlb_, 'price':price_, 'storage':quantity_, 'response':response, 'response_text':response_text, 'idx':idx}

#                                             =-=-=-=-=-=-=-=-=-=-=-= Execução =-=-=-=-=-=-=-=-=-=-=-=

def run_(
        _conta_1:bool=False,
        atualizar_todos:bool=False,
    ) -> None:

    if _conta_1:
        ACCESS_TOKEN = MLInterface._get_token(1)
        print(f"{'=-='*30}\nAtualizando conta 1\nTodos Produtos?: {atualizar_todos}\n")
    else:
        ACCESS_TOKEN = MLInterface._get_token(2)
        print(f"{'=-='*30}\nAtualizando conta 2\nTodos Produtos?: {atualizar_todos}\n")

    start_temp = perf_counter()
    
    # Chama o codigo "TodosProdutos"
    # SELECT de tabelas do Postegres
    df_produtos_postgres, df_estoqueProdutos_postgres, df_item_id_cost_frete_ml = read_tables_postgres(conta_1=_conta_1)
    # SELECT de tabelas EXCEL
    df_todosprodutos, df_mlbs_skus = read_tables_excel(conta_1=_conta_1)
    # Merge entre tabelas (Produto + Estoque)
    df_merge_produtos_estoque = merge_products_estoque(
        df_estoqueProdutos_postgres=df_estoqueProdutos_postgres,
        df_produtos_postgres=df_produtos_postgres
    )
    # Merge entre tabelas (Produto/Estoque + TodosProdutos_PKAIZEN)
    df_merge_produtos_estoque_preco = merge_products_estoque_price(
        df_merge_produtos_estoque=df_merge_produtos_estoque,
        df_todosprodutos=df_todosprodutos
    )
    # Merge entre tabelas (Produtos/PKAIZEN + ItemsIDs/ML)
    df_merge_produtos_estoque_preco_mlbs = merge_products_estoque_price_mlbs(
        df_merge_produtos_estoque_preco=df_merge_produtos_estoque_preco,
        df_mlbs_skus=df_mlbs_skus
    )
    # Merge entre tabela resultante dos 3 merges acima + custo de frete de cada item_id(MLB)
    df_merge_pkStockMlbs_mlbsCostFrete = merge_pkStockMlbs_mlbsCostFrete(
        df_merge_produtos_estoque_preco_mlbs=df_merge_produtos_estoque_preco_mlbs,
        df_item_id_cost_frete_ml1_or_ml2=df_item_id_cost_frete_ml
    )
    # Filtro do dataframe resultante de todas as funções
    df_merge_pkStockMlbs_mlbsCostFrete = df_merge_pkStockMlbs_mlbsCostFrete[['item_id', 'variation_id', 'variation_sku', 'num_fab', 'calc_price_ml', 'estoque']]
    df_merge_pkStockMlbs_mlbsCostFrete = df_merge_pkStockMlbs_mlbsCostFrete.query('item_id != "NA"').reset_index(drop=True)
    df_merge_pkStockMlbs_mlbsCostFrete = df_merge_pkStockMlbs_mlbsCostFrete.rename({'estoque':'storage', 'num_fab':'sku', 'calc_price_ml':'price'}, axis=1)
    df_merge_pkStockMlbs_mlbsCostFrete.price = df_merge_pkStockMlbs_mlbsCostFrete.price.apply( lambda x : float("{:.2f}".format(x)) )
    # Comparando dataframe resultante com o dataframe do banco de dados
    df_items_ids = comp(
        df_merge_pkStockMlbs_mlbsCostFrete=df_merge_pkStockMlbs_mlbsCostFrete,
        atualizar_todos_prod=atualizar_todos,
        storage_only=True,
        conta_1=_conta_1
        )
    print(f"\nIrei atualizar {len(df_items_ids['item_id'])}")
    # Dataframe axuliar, para receber os resultados das requisições
    df_aux = pd.DataFrame(columns=['item_id','price','storage','response','response_text'])
    
    # Execução das requisições
    with ProcessPoolExecutor() as exe:
        try:
            for future in tqdm(exe.map(interation, df_items_ids['item_id'], df_items_ids['price'], df_items_ids['storage'], df_items_ids.index, repeat(ACCESS_TOKEN)), total=len(df_items_ids)):
                # print(future['item_id'], future['response'], future['idx'])
                df_aux.loc[len(df_aux)] = future
        except Exception as error:
            print(f"\n\033[0;31m{error}\033[0m")
            pass

        finally:
            update_bank(
                df_aux.query('response == 200')[['item_id', 'price', 'storage']].reset_index(drop=True),
                conta_1=_conta_1)

    # Execucao das requiscoes pro FOR.
    # for idx in tqdm(df_items_ids.index):
    #     mlb = df_items_ids.loc[idx,'item_id']
    #     price = df_items_ids.loc[idx,'price']
    #     storage = df_items_ids.loc[idx,'storage']
        
    #     df_aux.loc[len(df_aux)] = interation(mlb, price, storage, idx, ACCESS_TOKEN)
        

    # Por fim, faz um Update dos dados ao banco, para comparação posteriomente
    # update_bank(
    #     df_aux.query('response == 200')[['item_id', 'price', 'storage']].reset_index(drop=True),
    #     conta_1=_conta_1)
    
    if len(df_aux.query('response != 200')) != 0:
        df_aux.query('response != 200').reset_index(drop=True).to_csv(f'temp/itens_nao_atualizado_{_conta_1}_{str(datetime.today().strftime("%m%d%Y_%H%M%S"))}.csv', index=False)

    end_temp = perf_counter()
    string_log = f'''
{datetime.today().strftime('%m/%d/%Y, %H:%M:%S')}
Quantidade de items_ids = {len(df_aux['item_id'])}
Quantidade de itens atualizados = {len(df_aux.query('response == 200'))}
Quantidade de itens não atualizados = {len(df_aux['item_id']) - len(df_aux.query('response == 200'))}
Tempo total de execução {convert_temp(seconds=int(end_temp-start_temp))}'''

    print(string_log)

    # msg_group(
    #     group_id='D41y7zYZ1AfFj22E0AEOUO',
    #     message=f"!!!! Mensagem Automatica !!!!\n\nConta Principal? = {_conta_1}\n{string_log}",
    #     tab_close=True,
    #     wait_time=7,
    #     close_time=3
    # )

    return None

if __name__ == '__main__':
    db = Postgres()
    run_(_conta_1=True, atualizar_todos=False)
    run_(_conta_1=False, atualizar_todos=False)
    db.close()