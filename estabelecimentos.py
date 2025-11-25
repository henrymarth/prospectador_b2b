import pandas as pd
import funcMegaMaxi as mm
banco = mm.get_engine()

# === 1. Lista de colunas oficiais do Estabelecimentos.csv ===
colunas_estabelecimentos = [
    "CNPJ_BASICO",
    "CNPJ_ORDEM",
    "CNPJ_DV",
    "IDENTIFICADOR_MATRIZ_FILIAL",
    "NOME_FANTASIA",
    "SITUACAO_CADASTRAL", #PRECISA DE TABELA DIMENSÃO: 01 – NULA 2 – ATIVA 3 – SUSPENSA 4 – INAPTA 08 – BAIXADA
    "DATA_SITUACAO_CADASTRAL",
    "MOTIVO_SITUACAO_CADASTRAL",
    "NOME_CIDADE_EXTERIOR",
    "PAIS",
    "DATA_INICIO_ATIVIDADE",
    "CNAE_FISCAL_PRINCIPAL",
    "CNAE_FISCAL_SECUNDARIA",
    "TIPO_LOGRADOURO",
    "LOGRADOURO",
    "NUMERO",
    "COMPLEMENTO",
    "BAIRRO",
    "CEP",
    "UF",
    "MUNICIPIO",#PRECISA DIMENSÃO - CSV QUE VEIO JUNTO
    "DDD1",
    "TELEFONE1",
    "DDD2",
    "TELEFONE2",
    "DDD_FAX",
    "FAX",
    "CORREIO_ELETRÔNICO",
    "SITUACAO_ESPECIAL",
    "DATA_SITUACAO_ESPECIAL"
]

qtdArquivos = mm.contar_arquivos('../arquivos', 'estabelecimentos')

print(qtdArquivos)

# === 4. Loop para ler os arquivos ===
for i in range(qtdArquivos):
    
    contador_temp = i+9
    arquivo = f"../arquivos/estabelecimentos{contador_temp}.csv"  # ajusta conforme sua pasta
    print(f"Lendo {arquivo}...")
    count = 0
    #faz loop sobre cada chunk
    for df_temp in pd.read_csv( 
        arquivo,
        encoding='ISO-8859-1',
        sep=";",
        dtype=str,
        names=colunas_estabelecimentos,# define nomes das colunas
        header=None,# diz que não existe cabeçalho no arquivo
        low_memory=False,
        chunksize = 500_000 #tamanho do chunk em linhas do excel
    ):

        #aqui eu devo subir a chunk para o banco de dados
        df_temp.to_sql(
            "estabelecimentos",
            banco,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5_000
        )
        count = count+1

        print(f"Chunk número {count} do DataFrame ")