import re
import pdfplumber
import pandas as pd
from collections import namedtuple
from datetime import datetime

from pydal import DAL, Field


LISTA_ANOS = [str(i) for i in range(1799,2022)]
#print(LISTA_ANOS)


db = DAL("mysql://*:*@rebec.mysql.pythonanywhere-services.com/*$*", 
         folder='', auto_import=True)




aqrquivo_pdf = ""

def retorna_indice_ano(lista_resultado):
    for ano in LISTA_ANOS:
        if ano in lista_resultado:
            return lista_resultado.index(ano)

def trata_data(date_time_str):
    date_time_obj = datetime.strptime(date_time_str, '%d/%m/%Y')
    return date_time_obj


def trata_tempo_prova(tempo_string):
    a = datetime.strptime(tempo_string, '%M:%S.%f')
    return a


def prova_categoria(string_prova):
    lista = string_prova.split()
    categoria = lista[-2]+lista[-1]

    try:
        genero = lista[-3]
    except:
        genero = None

    if "FEMININO" in categoria:
        categoria = categoria.replace("FEMININO", "")
        genero = "FEMININO"

    if "MASCULINO" in categoria:
        categoria = categoria.replace("MASCULINO", "")
        genero = "MASCULINO"


    prova = ""
    for i in lista[0:len(lista)-2]:
            prova = prova+i+" "
    prova = prova.strip()


    if "FEMININO" in prova:
        prova = prova.replace("FEMININO", "")

    if "MASCULINO" in prova:
        prova = prova.replace("MASCULINO", "")

    prova = prova.strip()

    categoria = categoria.strip()
    return {"prova": prova,
            "categoria": categoria,
            "genero": genero}


def checa_prova_linha(linha):
    if "PROVA" in line and "METROS" in line:
        nova_linha = linha.replace("PROVA", "")
        nova_linha = nova_linha.replace("-", "")
        lista_linha = nova_linha.split()
        sub = lista_linha[1:len(lista_linha)-1]
        nome_prova = ' '.join(sub)
        data_prova = trata_data(lista_linha[-1])


        prova_categoria_dic = prova_categoria(nome_prova)


        return {"prova": True,
                "nome_prova": prova_categoria_dic["prova"],
                "data_prova": data_prova,
                "categoria": prova_categoria_dic["categoria"],
                "genero": prova_categoria_dic["genero"]}
    else:
        return {"prova": False, "nome": None}




def checa_resultado_linha(linha):
    #"2º 4 5 DIEGO MIGUENS MELLO 374287 2014 TIJUCA TC/RJ 00:37.06 24,00 17
    lista_linha = linha.split()
    if "º" in lista_linha[0] and ":" in lista_linha[-3]:
        resultado = True
        posicao = int(lista_linha[0].replace("º", ""))
        try:
            serie = int(lista_linha[1])
        except:
            return {"resultado": False, "nome": None}
        try:
            raia = int(lista_linha[2])
        except:
            return {"resultado": False, "nome": None}

        indice_ano_nascimento = retorna_indice_ano(lista_linha)
        indice_registro = indice_ano_nascimento - 1
        registro = str(lista_linha[indice_registro])
        nascimento = int(lista_linha[indice_ano_nascimento])




        nome = ""
        for i in lista_linha[3:indice_registro]:
            nome = nome+i+" "
        nome = nome.strip()
        pontos = float(lista_linha[-2].replace(",", "."))
        tempo = lista_linha[-3]

        indice_tempo = lista_linha.index(tempo)

        equipe = ""
        for i in lista_linha[indice_ano_nascimento+1:indice_tempo]:
            equipe = equipe+i+" "
        equipe = equipe.strip()


        return {"resultado": True,
                "posicao": posicao,
                "serie": serie,
                "raia": raia,
                "nome": nome,
                "registro": registro,
                "nasc": nascimento,
                "tempo": trata_tempo_prova(tempo),
                "equipe": equipe
                }
    else:
        return {"resultado": False, "nome": None}





lista_arquivos = ["celebridadesagosto_FLAMENGO_CURTA.pdf",
"celebridadesnovembro_VASCO_LONGA.pdf",
"celebridadessetembro_FLUMINENSE_LONGA.pdf",
"celebridadessetembro_VASCO_LONGA.pdf",
"estadualinverno_MARINA_CURTA.pdf",
"estadualverao_VASCO_LONGA.pdf"]


for arquivo in lista_arquivos:
    lista = arquivo.split("_")
    ap = arquivo
    local = lista[1]
    piscina = lista[2].replace(".pdf", "")




    with pdfplumber.open(ap) as pdf:
        total_pages = len(pdf.pages)
        print(total_pages)
        ultima_prova = ""
        for i in range(0,total_pages):
            page = pdf.pages[i]
            text = page.extract_text()


            for line in text.split('\n'):
                checa_prova = checa_prova_linha(line)
                if checa_prova["prova"]:
                    nome_prova = checa_prova["nome_prova"]
                    data_prova = checa_prova["data_prova"]
                    categoria = checa_prova["categoria"]
                    genero = checa_prova["genero"]
                    ultima_prova = nome_prova

                checa_resultado = checa_resultado_linha(line)

                if checa_resultado["resultado"]:
                    checa_resultado["prova"] =  ultima_prova
                    checa_resultado["data_prova"] = data_prova
                    checa_resultado["categoria"] = categoria
                    checa_resultado["genero"] = genero
                    print(checa_resultado)
                    try:
                        db.resultados.insert(data_competicao = checa_resultado["data_prova"],
                                         prova = checa_resultado["prova"],
                                         categoria = checa_resultado["categoria"],
                                         genero = checa_resultado["genero"],
                                         nome = checa_resultado["nome"],
                                         registro = checa_resultado["registro"],
                                         nasc = checa_resultado["nasc"],
                                         equipe = checa_resultado["equipe"],
                                         posicao = checa_resultado["posicao"],
                                         tempo = checa_resultado["tempo"],
                                         serie = checa_resultado["serie"],
                                         raia = checa_resultado["raia"],
                                         local = local,
                                         piscina=piscina)
                        db.commit()
                    except:
                        continue

