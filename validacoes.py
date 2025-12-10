import re
import dns.resolver
import argparse
from sqlalchemy import text
import funcMegaMaxi as mm
from variaveis_pessoais import N8N_URL
import requests
banco = mm.get_engine()

#chamar n8n
def validar_telefone(numero, timeout = 15):
    payload = {"numero": numero}

    try:
        resposta = requests.post(N8N_URL, json=payload, timeout=timeout)
        resposta.raise_for_status()

        # tenta retornar JSON, senão texto
        try:
            return resposta.json()
        except ValueError:
            return resposta.text

    except Exception as e:
        print(f"Erro ao validar WhatsApp ({numero}): {e}")
        return None

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

    #criar o cnpj completo:
    cnpj_completo = f"{linha['CNPJ_BASICO']}{linha['CNPJ_ORDEM']}{linha['CNPJ_DV']}"

    #verifica DDD1 e TELEFONE1 e insere no banco se tiver tudo certo
    if linha['DDD1'] and str(linha['DDD1']).strip() != "":

        if linha['DDD1'] != '0000' and linha['TELEFONE1'] != '00000000' and linha['TELEFONE1'] != '0':
        
            numeroTel1 = f"55{linha['DDD1']}{linha['TELEFONE1']}"
            telefoneWppDictBool = validar_telefone(numeroTel1)
            tipo_contato = "Telefone"
            fonte = "Dados Abertos"
            #inserir no banco de dados:
            query = text("""
                INSERT INTO "contatos_empresa"
                (cnpj_basico, cnpj_completo, tipo_contato, valor_contato, fonte, telefone_tem_whatsapp)
                VALUES (:cnpj_basico, :cnpj_completo, :tipo_contato, :valor_contato, :fonte, :telefone_tem_whatsapp)
            """)
    
            with banco.connect() as conn:
                conn.execute(query, {
                    "cnpj_basico": linha["CNPJ_BASICO"],
                    "cnpj_completo": cnpj_completo,
                    "tipo_contato": tipo_contato,
                    "valor_contato": numeroTel1,
                    "fonte": fonte,
                    "telefone_tem_whatsapp": telefoneWppDictBool['numeroValido']
                })
                conn.commit()
        
        else:
            print("Telefone 1 não estava válido")

    else:
        print("Telefone 1 não estava válido")

    #verifica DDD2 e TELEFONE2 e insere no banco se tiver tudo certo
    if linha['DDD2'] and str(linha['DDD2']).strip() != "":

        if linha['DDD2'] != '0000' and linha['TELEFONE2'] != '00000000' and linha['TELEFONE2'] != '0':
        
            numeroTel1 = f"55{linha['DDD2']}{linha['TELEFONE2']}"
            telefoneWppDictBool = validar_telefone(numeroTel1)
            tipo_contato = "Telefone"
            fonte = "Dados Abertos"
            #inserir no banco de dados:
            query = text("""
                INSERT INTO "contatos_empresa"
                (cnpj_basico, cnpj_completo, tipo_contato, valor_contato, fonte, telefone_tem_whatsapp)
                VALUES (:cnpj_basico, :cnpj_completo, :tipo_contato, :valor_contato, :fonte, :telefone_tem_whatsapp)
            """)
    
            with banco.connect() as conn:
                conn.execute(query, {
                    "cnpj_basico": linha["CNPJ_BASICO"],
                    "cnpj_completo": cnpj_completo,
                    "tipo_contato": tipo_contato,
                    "valor_contato": numeroTel1,
                    "fonte": fonte,
                    "telefone_tem_whatsapp": telefoneWppDictBool['numeroValido']
                })
                conn.commit()
        
        else:
            print("Telefone 2 não estava válido")

    else:
        print("Telefone 2 não estava válido")



    #agora é validar o domínio de email
    #chamar a função, e fazer a query
    emailValidoBool = email_valido(linha['CORREIO_ELETRÔNICO'])
    print("email")
    print(emailValidoBool)

    tipo_contato = "Email"
    fonte = "Dados Abertos"
    #inserir no banco de dados:
    query = text("""
        INSERT INTO "contatos_empresa"
        (cnpj_basico, cnpj_completo, tipo_contato, valor_contato, fonte, sintaxe_valida, verificacao_externa_valida)
        VALUES (:cnpj_basico, :cnpj_completo, :tipo_contato, :valor_contato, :fonte, :sintaxe_valida, :verificacao_externa_valida)
    """)

    with banco.connect() as conn:
        conn.execute(query, {
            "cnpj_basico": linha["CNPJ_BASICO"],
            "cnpj_completo": cnpj_completo,
            "tipo_contato": tipo_contato,
            "valor_contato": linha['CORREIO_ELETRÔNICO'],
            "fonte": fonte,
            "sintaxe_valida": emailValidoBool,
            "verificacao_externa_valida": emailValidoBool
        })
        conn.commit()

print("cabou")