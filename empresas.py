import pandas as pd
import funcMegaMaxi as mm
banco = mm.get_engine()

# === 1. Lista de colunas oficiais do Estabelecimentos.csv ===
colunas_empresas = [
    "CNPJ_BASICO",
    "RAZAO_SOCIAL__NOME_EMPRESARIAL",
    "NATUREZA_JURIDICA",
    "QUALIFICAÇÃO_DO_RESPONSAVEL",
    "CAPITAL_SOCIAL_DA_EMPRESA",
    "PORTE_DA_EMPRESA",#PRECISA DE TABELA DIMENSÃO 00 – NÃO INFORMADO 01 - MICRO EMPRESA  03 - EMPRESA DE PEQUENO PORTE 05 - DEMAIS
    "ENTE_FEDERATIVO_RESPONSAVEL",
]

qtdArquivos = mm.contar_arquivos('../arquivos', 'empresas')

print(qtdArquivos)

# === 4. Loop para ler os arquivos ===
for i in range(qtdArquivos - 1):
    
    contador_temp = i
    arquivo = f"../arquivos/empresas{contador_temp}.csv"  # ajusta conforme sua pasta
    print(f"Lendo {arquivo}...")
    count = 0
    #faz loop sobre cada chunk
    for df_temp in pd.read_csv( 
        arquivo,
        encoding='ISO-8859-1',
        sep=";",
        dtype=str,
        names=colunas_empresas,# define nomes das colunas
        header=None,# diz que não existe cabeçalho no arquivo
        low_memory=False,
        chunksize = 500_000 #tamanho do chunk em linhas do excel
    ):

        #aqui eu devo subir a chunk para o banco de dados
        df_temp.to_sql(
            "empresas",
            banco,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5_000
        )
        count = count+1

        print(f"Chunk número {count} do DataFrame ")