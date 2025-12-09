import pandas as pd
import funcMegaMaxi as mm
banco = mm.get_engine()

# === 1. Lista de colunas oficiais do socios.csv ===
colunas_socios = [
    "CNPJ_BASICO",
    "IDENTIFICADOR_DE_SOCIO",
    "NOME_DO_SOCIO_OU_RAZAO_SOCIAL",
    "CNPJ_CPF_DO_SOCIO",
    "QUALIFICACAOO_DO_SOCIO",
    "DATA_DE_ENTRADA_SOCIEDADE",
    "PAIS",
    "REPRESENTANTE_LEGAL",
    "NOME_DO_REPRESENTANTE",
    "QUALIFICACAO_DO_REPRESENTANTE_LEGAL",
    "FAIXA_ETARIA"
]

qtdArquivos = mm.contar_arquivos('../arquivos', 'socios')

print(qtdArquivos)

# === 4. Loop para ler os arquivos ===
for i in range(qtdArquivos):
    
    contador_temp = i-1
    arquivo = f"../arquivos/socios{contador_temp}.csv"  # ajusta conforme sua pasta
    print(f"Lendo {arquivo}...")
    count = 0
    #faz loop sobre cada chunk
    for df_temp in pd.read_csv( 
        arquivo,
        encoding='ISO-8859-1',
        sep=";",
        dtype=str,
        names=colunas_socios,# define nomes das colunas
        header=None,# diz que não existe cabeçalho no arquivo
        low_memory=False,
        chunksize = 500_000 #tamanho do chunk em linhas do excel
    ):

        #aqui eu devo subir a chunk para o banco de dados
        df_temp.to_sql(
            "socios",
            banco,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000
        )

        count = count+1

        print(f"Chunk número {count} do DataFrame ")