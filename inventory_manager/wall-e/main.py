import pandas as pd
from sys import exit
from pywhatkit import sendwhatmsg_instantly as msg, sendwhatmsg_to_group_instantly as msg_group

from pecista import MLRepost


# Do nothing function
def do_nothing(*args, **kwargs):
    return args[0]

def parse_args():
    from argparse import ArgumentParser
    
    parser = ArgumentParser()
    
    # parser.add_argument('--grp_id', help='Id do grupo WhatsApp', required=True, type=str)
    parser.add_argument('--conta', help='Token de acesso a API ML', required=True, type=int)
    parser.add_argument('--ml', help='Arquivo com anúncios ML com histórico .csv', required=True, type=str)
    parser.add_argument('-o', '--out', help='Arquivo de saída dos dados de execução .csv', required=True, type=str)
    parser.add_argument('--max', help='int : Máximo de anúncios a postar', required=False, type=int, default=200)
    parser.add_argument('-v', default=False, action='store_true',
        help='Se presente, apresenta os passos de execução')
    
    args = parser.parse_args()
    
    # Validate args
    MLRepost(args.conta)
    
    if not args.ml.endswith('.csv') or not args.out.endswith('.csv'):
        print('Informe arquivos com a extensão válida (.csv --ml | .csv -o)')
        exit(1)
    
    return args

def send_msg_whatsapp(group_id_:str='D41y7zYZ1AfFj22E0AEOUO', conta_ml:int=0, string_log:int=0):

    msg_group(
        group_id=group_id_,
        message=f'''
        #### _Mensagem Automatica_ ####
        
        *Projeto WALL-E*
        Foram repostados {string_log} novos MLBs.
        Na conta {conta_ml}.''',
        tab_close=True,
        wait_time=7,
        close_time=3
    )

def main(item_ids:iter, ml_interface:MLRepost, out_file:str='out', log=do_nothing):

    df_out = pd.DataFrame(columns=['old_id', 'new_id', 'deleted', 'error'])

    log('Iniciando repostagem...')

    for item_id in item_ids:
        try:
            new_id, _, deleted = ml_interface.repost_item(item_id)
            df_out.loc[len(df_out)] = (item_id, new_id, deleted, None)
        except Exception as error:
            df_out.loc[len(df_out)] = (item_id, None, None, str(error))
    
    log(f'\t{len(df_out)} itens repostados')

    df_out.to_csv(out_file, index=False, sep=';')

    # send_msg_whatsapp(conta_ml=args.conta, string_log=len(df_out))

if __name__ == '__main__':

    # Parse arguments from terminal call
    args = parse_args()
    
    api = MLRepost(args.token)
    
    if args.v: # if verbose mode
        from tqdm import tqdm
    else:
        tqdm = do_nothing
        print = do_nothing

    # Main routine
    print('Leitura dos dados...')
    df_ml = pd.read_csv(args.ml, low_memory=False, sep=',')
    df_ml = df_ml[(df_ml['status'] == 'active') & (df_ml['sold_quantity'].astype(int) == 0) & (df_ml['variation_id'].isna())]
    df_ml = df_ml.sort_values(by='date_created').head(args.max)