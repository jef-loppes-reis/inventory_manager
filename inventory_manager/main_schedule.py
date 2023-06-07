from manager_invetory_main import run_
from pecista import Postgres
from time import sleep
from datetime import date, datetime
from os import system
from modules.cores import Cores
import schedule


def rodar_codigo():
    from os import system
    import subprocess
    # run manager inventory
    run_(_conta_1=True, atualizar_todos=False)
    sleep(30)
    system('py ./inventory_manager/kits_jogos.py')
    sleep(30)
    system('py ./inventory_manager/variables.py')
    sleep(30)
    run_(_conta_1=False, atualizar_todos=False)
    # run tb_venda
    sleep(30)
    print(
        '=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n'+
        'Rodando rotina de requisição dos dados de venda\n\n'
    )
    subprocess.run(
        ['python', r'C:\Users\jeferson.lopes\Documents\Python\tb_vendas\main.py'],
    stdout=subprocess.PIPE, shell=True)
    print('=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n')


# def get_all_mlbs(day:str):
#     from os import system
#     system('py ./src/get_infos_mlbs.py --out ./data/products/results_get_itens_conta_1.csv --conta 1')
#     system('py ./src/get_infos_mlbs.py --out ./data/products/results_get_itens_conta_2.csv --conta 2')
#     print(f'Chegou a {day}, importei todos produtos do ML!')

def get_all_prices_kaizen():
    system('py ./inventory_manager/copy_todos_proct.py --siac S:/TodosProdutos/ --out C:\\Users\\jeferson.lopes\\Documents\\Python\\manager_invetory\\data\\')

def refresh_ml_info():
    system('py ./inventory_manager/is_not_item_id.py --conta 1')
    system('py ./inventory_manager/is_not_item_id.py --conta 2')

def day_weekday():
    DIAS = [
        'Segunda-feira',
        'Terça-feira',
        'Quarta-feira',
        'Quinta-feira',
        'Sexta-feira',
        'Sábado',
        'Domingo'
    ]


    date_dia = date.today().strftime("%d")
    date_month = date.today().strftime("%m")
    date_year = date.today().strftime("%Y")

    data = date(year=int(date_year), month=int(date_month), day=int(date_dia))
    # print(data)

    indice_da_semana = data.weekday()
    # print(indice_da_semana)

    dia_da_semana = DIAS[indice_da_semana]
    # print(dia_da_semana)

    numero_do_dia_da_semana = data.isoweekday()
    # print(numero_do_dia_da_semana)

    return dia_da_semana


if __name__ == '__main__':

    # day_text = day_weekday()

    # if day_text == 'Sábado':

    #     schedule.every().day.at("08:00").do(get_all_prices_kaizen)
    #     schedule.every().day.at("08:10").do(refresh_ml_info)
    #     schedule.every().day.at("09:20").do(rodar_codigo)
    #     schedule.every().day.at("10:20").do(rodar_codigo)
    #     schedule.every().day.at("11:20").do(rodar_codigo)
    #     schedule.every().day.at("12:20").do(rodar_codigo)
    #     schedule.every().day.at("13:20").do(rodar_codigo)
    #     schedule.every().day.at("14:20").do(rodar_codigo)
    #     schedule.every().day.at("15:20").do(rodar_codigo)
    #     schedule.every().day.at("16:20").do(rodar_codigo)

    # elif day_text == 'Domingo':
    #     pass

    # else:
    #     schedule.every().day.at("08:00").do(get_all_prices_kaizen)
    #     schedule.every().day.at("08:10").do(refresh_ml_info)
    #     schedule.every().day.at("09:20").do(rodar_codigo)
    #     schedule.every().day.at("10:20").do(rodar_codigo)
    #     schedule.every().day.at("11:20").do(rodar_codigo)
    #     schedule.every().day.at("12:20").do(rodar_codigo)
    #     schedule.every().day.at("13:20").do(rodar_codigo)
    #     schedule.every().day.at("14:20").do(rodar_codigo)
    #     schedule.every().day.at("15:20").do(rodar_codigo)
    #     schedule.every().day.at("16:20").do(rodar_codigo)
    #     schedule.every().day.at("17:20").do(rodar_codigo)
    #     schedule.every().day.at("18:20").do(rodar_codigo)

    # while True:
    #     schedule.run_pending()
    #     sleep(1)


##### Exec por se txt conter horario determinado ######
    while True:
        with open("./data/postgres_updated.txt", "r") as arquivo:
            hora = int(arquivo.read())

        day_text = day_weekday()
        
        if hora == int(datetime.now().strftime("%H")):

            print(f"{Cores.AZUL}Iniciando rotina de estoque e preco...\n{datetime.now().strftime('[%H:%M:%S]')}{Cores.RESET}")
            with open ("./data/postgres_updated.txt", "w") as arquivo:
                arquivo.write("0")

            if (day_text == 'Quarta-feira') and (hora == 4):
                date_string = str(datetime.today().strftime("%Y%m%d"))
                log_args = f'py ./inventory_manager/wall-e/routine.py --conta 1 -o ./temp/wall-e_logs/result_{date_string}.csv --ml ./data/products/results_get_itens_conta_1.csv -v --max 200'
                system(log_args)

            if day_text == 'Sábado':
                if hora == 8:                 
                    get_all_prices_kaizen()
                    refresh_ml_info()
                if hora > 8 and hora < 16:
                    rodar_codigo()

            elif day_text == 'Domingo':
                pass

            else:
                if hora == 8:                 
                    get_all_prices_kaizen()
                    refresh_ml_info()
                if hora > 8 and hora < 19:
                    rodar_codigo()
            print(f"{Cores.VERDE}Codigo finalizado!\n{datetime.now().strftime('[%H:%M:%S]')}{Cores.RESET}")
            print('#-'*60)
        sleep(60)