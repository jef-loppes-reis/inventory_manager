from datetime import date
from shutil import copy
from os import remove, rename, path, system
import win32com.client as win32, win32com


def parse_args():
    from argparse import ArgumentParser
    
    parser = ArgumentParser()

    parser.add_argument('--siac', help='Caminho da plan. todosprodutos', required=True)
    parser.add_argument('--out', help='Saida do dado.', required=True)

    args = parser.parse_args()

    return args

def seErroRemoverPasta():
    print(win32com.__gen_path__)

def str_todosprodutos():

    data_atual = date.today().strftime('%Y_%m_%d')
    data_atual = 'todosProdutos-' + data_atual

    return data_atual

def copia_todos_produtos_do_sistema(endereco_entrada:str, endereco_saida:str):

    src = f'{endereco_entrada}' + f'{str_todosprodutos()}.xls'
    des = f'{endereco_saida}' + f'{str_todosprodutos()}.xls'

    print(f'\n{src} | Copiando...') 

    copy(src, des)

    print(f'\n{des} | Copiado !')

def converterxlx(end_input:str, end_out:str, fname:str, file_format:int, extecion_save:str='.csv'):
    """Converte o arquivo xls para xlsx, usando codigo da tabela "XlFileFormat enumeration (Excel)"

    Args:
        end_input (str): Recebe o endereco de entrada.
        end_out (str): Recebe o endereco de Saida.

    Exemplos:
        >>> end_out = 'C:\\Users\\jeferson.lopes\\Documents\\Python\\copia_todosProdutos_siac\\out'
        >>> fname='C:\\Users\\jeferson.lopes\\Documents\\Python\\copia_todosProdutos_siac\\data\\' + dataAtual()'
        >>> file_format = 62 # Codigo para que o Excel entenda em qual formato salvar o arquivo. (CSV)
        >>> extecion_save = '.csv'
    """

    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(fname + '.xls')

    print(f'\n{str(excel), str(wb)} | Trabalhando...')
    wb.SaveAs(fname + extecion_save, FileFormat = file_format)    #FileFormat = 51 is for .xlsx extension
    wb.Close()                               #FileFormat = 56 is for .xls extension
    excel.Application.Quit()
    print('Terminei !\n')

    remove(f'{end_out}\\{str_todosprodutos()}.xls')
    # rename(f'{end_out}\\{dataAtual()}.xlsx', f'{end_out}\\TodosProdutos.xlsx')

if __name__ == '__main__':

    args = parse_args()
    data = args.siac
    out = args.out

    data = str(data)

    print(data, out)
    system(f'del /q /f /s {out}*.csv')
    system(f'del /q /f /s {out}*.xls')

    copia_todos_produtos_do_sistema(f'{data}', f'{out}')

    converterxlx(
        end_input=out,
        end_out=out,
        fname=out + str_todosprodutos(),
        file_format= 62)