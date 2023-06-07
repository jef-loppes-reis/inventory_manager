import pandas as pd
import dateparser
from datetime import datetime, timedelta
from pytz import utc

from pecista import MLRepost
import main


DATE_NOW = datetime.now(utc).replace(microsecond=0)
DATE_TIME_DELTA = timedelta(days=120)

# Do nothing function
def do_nothing(*args, **kwargs):
    return args[0]

if __name__ == '__main__':
    # Parse arguments from terminal call
    args = main.parse_args()
    
    api = MLRepost(args.conta)
    
    if args.v: # if verbose mode
        from tqdm import tqdm
    else:
        tqdm = do_nothing
        print = do_nothing

    # Main routine
    print('Leitura dos dados...')
    df_ml = pd.read_csv(args.ml)
    df_ml = df_ml[df_ml['variation_id'].isna()] # Não pegar anúncios com variação

    df_ml['date_created'] = df_ml['date_created'].apply(lambda x : dateparser.parse(x))

    df_ml = df_ml[(df_ml['status'] == 'active') & (df_ml['sold_quantity'].astype(int) == 0) & (DATE_NOW - df_ml['date_created'] > DATE_TIME_DELTA)]
    df_ml = df_ml.sort_values(by='date_created').head(args.max)
    
    result_main = main.main(
        tqdm(df_ml.item_id, total=len(df_ml.item_id)),
        api,
        args.out,
        log=print
    )