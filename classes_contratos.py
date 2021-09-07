import datetime
from datetime import datetime
import pandas as pd
import os
import time
import dotenv
import requests


def run_except(func):
        def inner_func(*args, **kwargs):
            try:
                #print('decorator ON - modo teste para poucas páginas')
                resultado = func(*args, **kwargs)
            except Exception as e:
                print(f'Erro: {e}')
                time.sleep(90)

                try:
                    resultado = func(*args, **kwargs)
                except Exception as e:
                    print(f'Erro de NOVO: {e}')
                    time.sleep(70)
                    pass
                else:
                    return resultado              
            else:
                return resultado
        return inner_func


class RequestApi():

    def __init__(self, tabela) -> None:
        self.base_endpoint = "https://gatewayapi.prodam.sp.gov.br:443/financas/orcamento/sof/v3.0.1"
        self.lstContratos = "lstContratos"
        self.tabela = tabela
              
    def __token_head(self) -> None:
        dotenv.load_dotenv(dotenv.find_dotenv())
        token = os.getenv("TOKEN")
        headers = {"Authorization": str("Bearer " + token)}
        return headers

    def _get_endpoint_init(self) -> str:
        return f"{self.base_endpoint}/{self.tabela}"

    def _consult_data(self, ano) -> dict:
        endpoint = self._get_endpoint_init()
        ano_consulta = int(ano)
        header = self.__token_head()
        params = {'anoContrato': ano_consulta}
        self.request_contratos = requests.get(endpoint, params=params, headers=header, verify=True)
        self.endpoint_puro = self.request_contratos.url
        self.request_contratos.raise_for_status()
        request_json = self.request_contratos.json()
        return request_json
        
    @property
    def consult_data_status(self) -> str:
        return self.request_contratos
    
    def get_data(self, ano_0:int) -> dict:
        self.novo_ano = ano_0
        raw_data = self._consult_data(self.novo_ano)
        indice = self.lstContratos
        return raw_data[indice]
   
    @run_except
    def get_data_pag(self, pagina:int) -> list:
        self.pagina = int(pagina)
        page = f"&numPagina={self.pagina}"
        endpoint_pagina = f"{self.endpoint_puro}{page}"
        self.run_get = requests.get(endpoint_pagina, headers=self.__token_head(), verify=True)
        self.run_get.raise_for_status()
        run_get_json = self.run_get.json()
        return run_get_json
          
    @property
    def get_data_pag_status(self) -> str:
        return self.run_get
    
    def verify_list(self, dict_append:dict, list_raw:list) -> list:

        # *Dict must be that from 'lstContratos' index        
        if isinstance(dict_append, list):
            final_list = list_raw + dict_append
            return final_list

        elif isinstance(dict_append, dict):
            type_change = [dict_append]
            final_list = list_raw + type_change
            return final_list


def criar_pastas_necessarias():
    """
    *Here we create all the paths needed to run all steps
    """
    dir_name = os.path.join(os.getcwd(), "Contratos_captura", "toda_coleta")
    print("Pasta local do arquivo main.py ", os.getcwd())
    dir_logs = os.path.join(os.getcwd(), "Contratos_captura", "dir_logs")


    if not (os.path.exists(dir_name) and os.path.exists(dir_logs)):
        os.makedirs(dir_name, exist_ok=True)
        os.makedirs(dir_logs, exist_ok=True)        
    else:
        print(f'Tudo certo, já existem os diretórios " {dir_name} " e " {dir_logs} " !')


class CarimboDiaHora:

    def marcar_dia_hora(self) -> str:
        """ ! Return tuple with day and hour"""       
        day_0 = time.strftime("%d-%m-%Y", time.localtime())
        time_0 = time.strftime("%I:%M:%S %p", time.localtime())
        return day_0, time_0

    def dia_datetime(self, dia):
        day_datetime = pd.to_datetime(dia)
        return day_datetime

    def hora_datetime(self, hora):
        hour_datetime = pd.to_datetime(hora)
        return hour_datetime

    def delta_tempo(self, inicio: datetime, fim: datetime) -> datetime:        
        delta = fim - inicio
        return print(delta)


class AlteraDF():

    def __init__(self, df) -> None:
        self.coleta = datetime.now()
        self.df_pagina = df

    def add_data_df(self):
        self.df_pagina['coletado'] = self.coleta.strftime('%d/%m/%y')
        return self.df_pagina
    
    def dedupe_df(self):
        self.df_novo = self.add_data_df()
        df_raiz = pd.read_csv("contratos_mestre_concat.csv")
        cols = self.df_novo.columns
        colunas=cols[:-1].tolist()
        df_puro = pd.concat([df_raiz, self.df_novo]).drop_duplicates(subset=colunas,keep="first")
        return df_puro

    def unique_data_df(self):
        df_completo = self.df_pagina
        dia_default = datetime.now().strftime('%d/%m/%y')
        return df_completo[df_completo['coletado'] == dia_default]
        

class SalvaArquivo:

    def salvar_log_texto(hora, dia):
        # abrindo arquivo txt para fazer registros de inicio e erros
        with open("Contratos_captura/dir_logs/log_erros.txt", "a") as file_error:
            file_error.write(
                f"\n\nScript iniciado as {str(hora)} dia {str(dia)}"
            )

    def salvar_log_texto_fim(self, hfinal:str , dfinal:str)-> None:
        with open("Contratos_captura/dir_logs/log_erros.txt", "a") as file:
            file.write(
                f"\n\nScript finalizado as {hfinal} dia {dfinal}"
            )
    
    def salva_page_error(self, lista_erros, dia_erro) -> None:
        with open ("Contratos_captura/dir_logs/log_page_error.txt", "a") as page_error:
            page_error.write(
                f"{dia_erro}, {lista_erros}\n"
            )

    def salva_df(self,df_name, diacoleta:str) -> None:
        self.df = df_name
        self.name = f"contrato_{diacoleta}.csv"
        dir_name = "Contratos_captura/toda_coleta"
        self.df.to_csv(os.path.join(os.getcwd(),dir_name, self.name), index=False)

    def salva_df_nu_batch(self, df_new) -> None:
        self.dfnu = df_new
        name_batch = "contratos_dados_batch.csv"
        dir_batch = "Contratos_captura/toda_coleta"
        self.dfnu.to_csv(os.path.join(os.getcwd(), dir_batch, name_batch), mode='w', index=False)

    def append_data_df(self, dataframe) -> None:
        df = dataframe
        df.to_csv('contratos_mestre_concat.csv', mode='a', header=None, index=False)




