from datetime import date
import os
import re
import pandas as pd


class Saneamento:
    
    def __init__(self, data, configs, tabela):
        self.data = data
        self.metadado =  pd.read_excel(configs[tabela]["meta_path"])
        self.len_cols = max(list(self.metadado["id"]))
        self.colunas = list(self.metadado['nome_original'])
        self.colunas_new = list(self.metadado['nome'])
        self.path_work = configs[tabela]["path_work"]

    def rename(self):
        for i in range(self.len_cols):
            self.data.rename(columns={self.colunas[i]:self.colunas_new[i]}, inplace = True)
    
    def normalize_dtype(self):
        for col in self.colunas_new:
            self.data[col] = self.data[col].astype(str)

    def normalize_null(self):
        for col in self.colunas_new:
            self.data[col].replace("unknown", None, regex=True, inplace = True)
            self.data[col].replace("n/a", None, regex=True, inplace = True)

    def tipagem(self):
        for col in self.colunas_new:
            tipo = self.metadado.loc[self.metadado['nome'] == col]['tipo'].item()
            if tipo == "int":
                tipo = self.data[col].astype(int)
            elif tipo == "float":
                self.data[col].replace(",", ".", regex=True, inplace = True)
                self.data[col] = self.data[col].astype(float)
            elif tipo == "date":
                self.data[col] = pd.to_datetime(self.data[col])
    
    def normalize_str(self):
        for col in self.colunas_new:
            tipo = self.metadado.loc[self.metadado['nome'] == col]['tipo'].item()
            if tipo == "string":
                self.data[col] = self.data[col].apply(
                    lambda x: x.encode('ASCII', 'ignore')\
                        .decode("utf-8").lower() if x != None else None)

    def save_work(self):
        self.data["load_date"] = date.today()
        if not os.path.exists(self.path_work):
            self.data.to_csv(self.path_work, index=False, sep = ";")
        else:
            self.data.to_csv(self.path_work, index=False, mode='a', header=False, sep = ";")



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