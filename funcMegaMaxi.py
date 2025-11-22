import glob
from sqlalchemy import create_engine

from variaveis_pessoais import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

#contar a quantidade de arquivos de empresa
def contar_arquivos(pasta, prefixo, extensao="csv"):
    padrao = f"{pasta}/{prefixo}*.{extensao}"
    return len(glob.glob(padrao))

def get_engine():
    user = DB_USER
    password = DB_PASSWORD
    host = DB_HOST     # ou IP/VPS
    port = DB_PORT
    database = DB_NAME       # o banco que você já criou

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url)
    return engine