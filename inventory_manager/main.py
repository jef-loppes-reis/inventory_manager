from manager_invetory_main import run_
from pecista import Postgres
from time import sleep
from datetime import date
import schedule


def rodar_codigo():
    from os import system
    run_(_conta_1=True, atualizar_todos=False)
    system('py ./src/kits_jogos.py')
    run_(_conta_1=False, atualizar_todos=False)

# def get_all_mlbs(day:str):
#     from os import system
#     system('py ./src/get_infos_mlbs.py --out ./data/products/results_get_itens_conta_1.csv --conta 1')
#     system('py ./src/get_infos_mlbs.py --out ./data/products/results_get_itens_conta_2.csv --conta 2')
#     print(f'Chegou a {day}, importei todos produtos do ML!')

def get_all_prices_kaizen():
    from os import system
    system('py ./src/copy_todos_proct.py --siac S:/TodosProdutos/ --out C:\\Users\\jeferson.lopes\\Documents\\Python\\manager_invetory\\data\\')

def refresh_ml_info():
    from os import system
    system('py ./src/is_not_item_id.py --conta 1')
    system('py ./src/is_not_item_id.py --conta 2')


rodar_codigo()