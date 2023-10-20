from datetime import date, datetime
import os
import re
import pandas as pd
from config import logs_file


def split_string_list(data, col, qtd):
    df = data.copy()
    df['col_temp'] = df[col].map(lambda x: x.strip('][').split(', '))
    if qtd:
        df[col] = df['col_temp'].apply(lambda x: 0 if x==[''] else len([y.replace("'", "") for y in x ]) )
    else:
        df[col] = df['col_temp'].apply(lambda x: 0 if x==[''] else [y.replace("'", "") for y in x ] )

    return df[col]

def sw_work_to_dw(configs_work, configs_dw):
    people = pd.read_csv(configs_work['people']['path_work'], sep=";")
    planets = pd.read_csv(configs_work['planets']['path_work'], sep=";")
    films = pd.read_csv(configs_work['films']['path_work'], sep=";")

    people['qtd_veiculos'] = split_string_list(people, 'url_vehicles', True)
    people['qtd_naves'] = split_string_list(people, 'url_starships', True)
    people['url_films'] = split_string_list(people, 'url_films', False)

    films['titulo_filme2'] = films['titulo_filme'].map(lambda x: x.lower().replace(" ", "_"))
    film_dict = films[['titulo_filme2', 'url_origem']].set_index('titulo_filme2').to_dict()['url_origem']
    
    for key in film_dict:
        people[key] = people['url_films'].apply(lambda x: 1 if film_dict[key] in x else 0)

    df = people.merge(
        planets[['nome_planeta', 'url_origem']], left_on='url_homeworld', right_on='url_origem'
        ).rename(columns={"nome_planeta": 'planeta_natal'})
    
    sw = df[list(pd.read_excel(configs_dw['meta_path'])['nome'])]    
    sw.to_csv(configs_dw['dw_path'], index=False)
    

    return True

def error_handler(exception_error, table, stage):
        
        log = [stage, type(exception_error).__name__, exception_error,
               table, datetime.now()]
        logdf = pd.DataFrame(log).T
        
        if not os.path.exists(logs_file):
            logdf.columns = ['stage', 'type', 'error', 'table', 'datetime']
            logdf.to_csv(logs_file, index=False,sep = ";")
        else:
            logdf.to_csv(logs_file, index=False, mode='a', header=False, sep = ";")