import classes_contratos as ct
import pandas as pd
import logging
from anos import anos

# Criando diretorios necessarios
ct.criar_pastas_necessarias()

# Carregando data e hora do inicio do script
dia_0, hora_0 = ct.CarimboDiaHora().marcar_dia_hora()

# Criar arquivo de texto para logs de inicio e fim do script
ct.SalvaArquivo.salvar_log_texto(hora_0, dia_0)

# Criando classe de conexao
connect = ct.RequestApi("contratos")

lista_de_anos_contrato = anos
df_pagina = pd.DataFrame()
list_page_error = []

for x in lista_de_anos_contrato:

    # Consulta sendo convertida em JSON
    consult_data = connect._consult_data(x)

    if consult_data["lstContratos"] != None:

        # Verifica numero de paginas da consulta
        number_of_pages = consult_data["metadados"]["qtdPaginas"]

        # Criacao de lista de dict
        todos_contratos = []
        todos_contratos = connect.verify_list(
            consult_data["lstContratos"], todos_contratos
        )

        # Configurando Logging
        logging.basicConfig(
            filename="Contratos_captura/dir_logs/Log_Erros_Contratos.log",
            filemode="a",
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s:%(message)s",
        )

        if number_of_pages > 1:

            for p in range(2, number_of_pages+1):

                teste_get_page = connect.get_data_pag(p)

                if teste_get_page != None:
                    todos_contratos = connect.verify_list(
                        teste_get_page["lstContratos"], todos_contratos
                    )
                else:
                    print(f"Erro na página {p}")
                    logging.error(f"Falha na pagina {p}")
                    list_page_error.append(p)

        df_contratos_temp = pd.DataFrame(todos_contratos)
        df_pagina = pd.concat([df_pagina, df_contratos_temp])


print(
    f"\nArquivo coletado tem {df_pagina.shape[0]} linhas e {df_pagina.shape[1]} colunas"
)

#! Gera DF de dados novos + antigos - valores unicos
dfpuro = ct.AlteraDF(df_pagina).dedupe_df()
#! Gera DF com valores do dia de coleta - valores unicos
df_nu = ct.AlteraDF(dfpuro).unique_data_df()

#! Gera CSV com dados atualizados e completos
ct.SalvaArquivo().salva_df(df_nu, dia_0)
#! Adiciona os dados novos e unicos ao csv mestre
ct.SalvaArquivo().append_data_df(df_nu)
#! Salva dados únicos e novos para load_to_RDS : batch
ct.SalvaArquivo().salva_df_nu_batch(df_nu)

dia_f, hora_f = ct.CarimboDiaHora().marcar_dia_hora()
ct.SalvaArquivo().salvar_log_texto_fim(hora_f, dia_f)
if list_page_error is not None:
    ct.SalvaArquivo().salva_page_error(list_page_error, dia_0)
print("Script Encerrado com Sucesso")