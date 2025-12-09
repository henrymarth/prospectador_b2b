import re
import dns.resolver
import argparse
from sqlalchemy import text
import funcMegaMaxi as mm
import requests
banco = mm.get_engine()

def buscar_estabelecimentos_por_cidade(codigo_cidade):
    query = text("""
        SELECT "CNPJ_BASICO", "CNPJ_ORDEM", "CNPJ_DV",
               "DDD1", "TELEFONE1", "DDD2", "TELEFONE2",
               "CORREIO_ELETRÔNICO"
        FROM "estabelecimentos"
        WHERE "SITUACAO_CADASTRAL" = '02'
          AND "MUNICIPIO" = :cidade
    """)

    with banco.connect() as conn:
        resultado = conn.execute(query, {"cidade": codigo_cidade})
        registros = [dict(r) for r in resultado.mappings()]
    
    return registros


def validar_wpp(numero):

    url = "suspence"
    #só fazer a request aqui
    #e retornar o valor

def email_valido(email):
    # valida formato
    if not re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email):
        return False

    dominio = email.split("@")[1]

    # valida MX
    try:
        dns.resolver.resolve(dominio, 'MX')
        return True
    except:
        return False


#-------------------------------------------------------------------------------------------------------------------------------
#fazer um select de todos os estabelecimentos dessa cidade, trazendo cnpj básico, cnpj_completo, telefone1, telefone2, email
#fazer um loop sobre cada resultado, verifica se tem numero, valida numero insere numero, verifica se tem email, valida email, insere email


parser = argparse.ArgumentParser(description="Validador por cidade")

parser.add_argument(
    "--cidade",
    type=str,
    required=True,
    help="Código da cidade a ser validada"
)

args = parser.parse_args()
codigo_cidade = args.cidade
print(f"Executando validações para a cidade {codigo_cidade}...")

dados = buscar_estabelecimentos_por_cidade(codigo_cidade)

for linha in dados:
    print("to no loop")
    print(linha["CNPJ_BASICO"])
    break

print("cabou")