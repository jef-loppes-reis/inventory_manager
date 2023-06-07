import asyncio
import aiohttp
from os import path
from time import perf_counter
import pandas as pd
from datetime import datetime

from pecista import MLInterface 


result_keys = (
    'item_id',
    'item_sku',
    'variation_id',
    'variation_sku',
    'title',
    'available_quantity',
    'sold_quantity',
    'price',
    'status',
    'date_created',
    'category_id'
)

total = 0
done = 0


async def list_items(item_ids:asyncio.Queue, ml:MLInterface, limit:int=float('inf')):
    url = f'{MLInterface.BASE_URL}/users/{ml._user_id}/items/search'
    querystring = {
        "search_type": "scan",
        "orders": "start_time_desc"
    }
    
    res = None
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=querystring, headers=ml._headers()) as res:
            res = await res.json()

        querystring = {
            **querystring,
            'scroll_id': res['scroll_id']
        }
        items = res['results']
        
        for item_id in res['results']:
            item_ids.put_nowait(item_id)
        
        global total, listed
        total = min(res['paging']['total'], limit)

        while len(items) < limit:
            async with session.get(url, params=querystring, headers=ml._headers()) as res:
                res = await res.json()
            if not res['results']:
                break
            items.extend(res['results'])
            for item_id in res['results']:
                item_ids.put_nowait(item_id)

        return items


async def query_items(item_ids:asyncio.Queue, ml:MLInterface, items:asyncio.Queue, errors:list):
    while True:
        item_id = await item_ids.get()
        res = None
        count = 0
        try:
            while True:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f'https://api.mercadolibre.com/items/{item_id}?include_attributes=all', headers=ml._headers()) as res:
                        res = await res.json()
                        if 'id' in res.keys():
                            items.put_nowait(res)
                            break
                count += 1
                if count == 12:
                    raise TimeoutError()
                await asyncio.sleep(5)
        except:
            item_ids.put_nowait(item_id)
            await asyncio.sleep(1)
        
        item_ids.task_done()

def search_attribute(attribute_id:str, attributes:list):
    for attribute in attributes:
        if attribute['id'] == attribute_id:
            return attribute['value_name']


async def filter_item_info(items:asyncio.Queue, results:list, errors:list):
    while True:
        item = await items.get()
        
        try:
            # if not item['status'] in ['active', 'paused']:
            #     items.task_done()
            #     continue
            
            result = { key: '' for key in result_keys }
            for key in [ key for key in result_keys if key in item.keys() ]:
                result[key] = item[key]
            result['item_id'] = item['id']
            if 'attributes' in item.keys():
                result['item_sku'] = search_attribute('SELLER_SKU', item['attributes'])
            
            if not ('variations' in item.keys() and item['variations']):
                results.append(result)
            else:
                for variation in item['variations']:
                    result_var = {
                        **result
                    }
                    result_var['variation_id'] = variation['id']
                    if 'attributes' in variation.keys():
                        result_var['variation_sku'] = search_attribute('SELLER_SKU', variation['attributes'])
                    results.append(result_var)
            
            global done
            done += 1
        except:
            errors.append(item)
            global total
            total -= 1
        
        items.task_done()


def empty_queue(q: asyncio.Queue):
    while not q.empty():
        q.get_nowait()
        q.task_done()


async def main(filename:str, conta:int, limit:int, timeout:float):
    _start = perf_counter()
    
    N = 50
    
    ml = MLInterface(conta)
    item_ids = asyncio.Queue()
    items = asyncio.Queue()
    results = []
    errors = []

    tasks = []
    for _ in range(N):
        tasks.append(asyncio.create_task(query_items(item_ids, ml, items, errors)))
    
    for _ in range(N):
        tasks.append(asyncio.create_task(filter_item_info(items, results, errors)))
        
    tasks.append(asyncio.create_task(list_items(item_ids, ml, limit)))
    
    global total, done
    while total == 0:
        await asyncio.sleep(.5)
    
    while done < total:
        await asyncio.sleep(.125)
        print(datetime.now().strftime('[%H:%M:%S] ') + f'{done}/{total}', end='\r')
        if perf_counter() - _start > timeout * 60:
            break
    await asyncio.sleep(.125)
    print(datetime.now().strftime('[%H:%M:%S] ') + f'{done}/{total}')
    
    if not (perf_counter() - _start > timeout * 60):
        await item_ids.join()
        await items.join()
    else:
        print('Tempo limite alcançado, tarefas canceladas.')

    for task in tasks:
        task.cancel()
    empty_queue(item_ids)
    empty_queue(items)

    df = pd.DataFrame.from_records(results).sort_values(by=['item_id', 'variation_id'])
    
    if filename.endswith('.csv'):
        df.to_csv(filename, index=False)
    elif filename.endswith('.feather'):
        df.reset_index(drop=True).astype(str).to_feather(filename)
    
    if len(errors) > 0:
        with open('.'.join(filename.split('.')[:-1]) + '_errors.json', 'w') as fp:
            from json import dumps
            fp.write(dumps(errors))
    
    print(f"Itens salvos = {len(df['item_id'].unique())}")
    print(f'Tempo total = {perf_counter() - _start:.2f}s')
    
    try:
        await asyncio.gather(*tasks, return_exceptions=False)
    except:
        pass


if __name__ == '__main__':
    from argparse import ArgumentParser
    
    parser = ArgumentParser()
    parser.add_argument('-o', '--out', help='Arquivo de saída .csv', required=True, type=str)
    parser.add_argument('-c', '--conta', help='Conta do ML: [1 ou 2]', required=True, type=int)
    parser.add_argument('-l', '--limit', help='Quantidade de itens buscados', required=False, type=int)
    parser.add_argument('-t', '--timeout', help='Tempo máximo de execução [min]', required=False, type=float, default=10)
    args = parser.parse_args()
    args.out = args.out.replace('\\', '/')
    
    if not (args.out.endswith('.csv') or args.out.endswith('.feather')):
        print('Informe um nome de arquivo com a extensão válida (.csv, .feather)')
    elif '/'.join(args.out.split('/')[:-1]) != '' and not path.exists('/'.join(args.out.split('/')[:-1])):
        print(f"A pasta '{'/'.join(args.out.split('/')[:-1])}' não existe!")
    else:
        asyncio.run(main(args.out, args.conta, args.limit if args.limit else float('inf'), args.timeout))
