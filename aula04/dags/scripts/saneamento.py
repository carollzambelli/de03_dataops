import pandas as pd
import os
from datetime import date
from utils import error_handler    


class Saneamento:

    def __init__(self, data, configs, tabela):
        self.tabela = tabela
        self.data = data
        self.metadado =  pd.read_excel(configs[self.tabela]["meta_path"])
        self.len_cols = max(list(self.metadado["id"]))
        self.colunas = list(self.metadado['nome_original'])
        self.colunas_new = list(self.metadado['nome'])
        self.path_work = configs[tabela]["path_work"]

    def rename(self):
        try:
            for i in range(self.len_cols):
                self.data.rename(columns={self.colunas[i]:self.colunas_new[i]}, inplace = True)
        except Exception as exception_error:
            error_handler(exception_error, self.tabela, 'preparation')
    
    def normalize_dtype(self):
        try:
            for col in self.colunas_new:
                self.data[col] = self.data[col].astype(str)
        except Exception as exception_error:
            error_handler(exception_error, self.tabela, 'preparation')

    def normalize_null(self):
        try:
            for col in self.colunas_new:
                self.data[col].replace("unknown", None, regex=True, inplace = True)
                self.data[col].replace("n/a", None, regex=True, inplace = True)
        except Exception as exception_error:
            error_handler(exception_error, self.tabela, 'preparation')

    def tipagem(self):
        try:
            for col in self.colunas_new:
                tipo = self.metadado.loc[self.metadado['nome'] == col]['tipo'].item()
                if tipo == "int":
                    tipo = self.data[col].astype(int)
                elif tipo == "float":
                    self.data[col].replace(",", ".", regex=True, inplace = True)
                    self.data[col] = self.data[col].astype(float)
                elif tipo == "date":
                    self.data[col] = pd.to_datetime(self.data[col])
        except Exception as exception_error:
            error_handler(exception_error, self.tabela, 'preparation')
    
    def normalize_str(self):
        try: 
            for col in self.colunas_new:
                tipo = self.metadado.loc[self.metadado['nome'] == col]['tipo'].item()
                if tipo == "string":
                    self.data[col] = self.data[col].apply(
                        lambda x: x.encode('ASCII', 'ignore')\
                            .decode("utf-8").lower() if x != None else None)
        except Exception as exception_error:
            error_handler(exception_error, self.tabela, 'preparation')

    def null_tolerance(self):
        for col in self.colunas_new:
            nul = self.metadado.loc[self.metadado['nome'] == col]['permite_nulo'].item()
            if int(nul) == 0:
                if len(self.data[self.data[col].isna()]) > 0:
                    raise Exception(f"{self.tabela} possui nulos acima do permitido")

    def save_work(self):
        try:
            self.data["load_date"] = date.today()
            if not os.path.exists(self.path_work):
                self.data.to_csv(self.path_work, index=False, sep = ";")
            else:
                self.data.to_csv(self.path_work, index=False, mode='a', header=False, sep = ";")
        except Exception as exception_error:
            error_handler(exception_error, self.tabela, 'preparation')