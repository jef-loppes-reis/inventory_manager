from datetime import date, datetime
import schedule
from time import sleep
import os


def day_weekday():
    DIAS = [
        'Segunda-Feira',
        'Terça-Feira',
        'Quarta-Feira',
        'Quinta-Feira',
        'Sexta-Feira',
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

def main():
    date_string = str(datetime.today().strftime("%Y%m%d"))
    log_args = f'py ./src/routine.py --conta 1 -o ./out/result_{date_string}.csv --ml ./data/results_get_itens_conta_1.csv -v'
    os.system(log_args)


if __name__ == '__main__':

    day_text = day_weekday()

    if day_text == 'Quarta-Feira':
        schedule.every().day.at("09:40").do(main)

    while True:
        schedule.run_pending()
        sleep(1)