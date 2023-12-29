'''
create unlogged table movimentacao(linhas integer, competenciamov integer, regiao integer, uf integer, municipio integer, secao varchar(2), subclasse integer, saldomovimentacao integer, cbo2002ocupacao integer, categoria integer, graudeinstrucao integer, idade varchar(20), horascontratuais varchar(20), racacor integer, sexo integer, tipoempregador integer, tipoestabelecimento integer, tipomovimentacao integer, tipodedeficiencia integer, indtrabintermitente integer, indtrabparcial integer, salario varchar(20), tamestabjan integer, indicadoraprendiz integer, origemdainformacao integer, competenciadec integer,  indicadordeforadoprazo integer, unidadesalariocodigo integer, valorsalariofixo varchar(20), arquivo varchar(20), fxetaria integer, arg_hash varchar(250), hash varchar(50), processado integer default 0, id serial primary key);


create unlogged table movimentacao_exc(linhas integer, competenciamov integer, regiao integer, uf integer, municipio integer, secao varchar(2), subclasse integer, saldomovimentacao integer, cbo2002ocupacao integer, categoria integer, graudeinstrucao integer, idade varchar(20), horascontratuais varchar(20), racacor integer, sexo integer, tipoempregador integer, tipoestabelecimento integer, tipomovimentacao integer, tipodedeficiencia integer, indtrabintermitente integer, indtrabparcial integer, salario varchar(20), tamestabjan integer, indicadoraprendiz integer, origemdainformacao integer, competenciadec integer, competenciaexc integer, indicadordeexclusao integer, indicadordeforadoprazo integer, unidadesalariocodigo integer, valorsalariofixo varchar(20), arquivo varchar(20), fxetaria integer, arg_hash varchar(250), hash varchar(50), processado integer default 0, id serial primary key);


'''



import py7zr
import pandas as pd
import psycopg2 as postgres
import traceback
import json
import math
import os
import time
import hashlib
import traceback
import gc

#Inserindo os registros com inser deixa o processo muito lento. Copy do postgres e mais rapido
def gravando_registros(dados: list):
    try:
        with postgres.connect(database="caged",
                        host="localhost",
                        #host="macbeth",
                        user="postgres",
                        password="",
                        port="5432") as con:
            with con.cursor() as cur:
                sql = '''INSERT INTO movimentacao(arquivo, hash, competencia, regiao, uf,
    municipio, secao, subclasse, saldomovimentacao, cbo2002ocupacao, categoria,
    graudeinstrucao, idade, fxetaria, horascontratuais, racacor, sexo, tipoempregador,
    tipoestabelecimento, tipomovimentacao, tipodedeficiencia, indtrabintermitente,
    indtrabparcial, salario, tamestabjan, indicadoraprendiz, origemdainformacao,
    competenciadec, competenciaexc, indicadordeexclusao, indicadordeforadoprazo, unidadesalariocodigo, valorsalariofixo) 
    values(%(arquivo)s, %(hash)s, %(competênciamov)s, %(região)s, %(uf)s, %(município)s, %(seção)s, %(subclasse)s, %(saldomovimentação)s, %(cbo2002ocupação)s, %(categoria)s, %(graudeinstrução)s, %(idade)s, %(fxetaria)s, %(horascontratuais)s, %(raçacor)s, %(sexo)s, %(tipoempregador)s, %(tipoestabelecimento)s, %(tipomovimentação)s, %(tipodedeficiência)s, %(indtrabintermitente)s, %(indtrabparcial)s, %(salário)s, %(tamestabjan)s, %(indicadoraprendiz)s, %(origemdainformação)s, %(competênciadec)s, %(competenciaexc)s, %(indicadordeexclusao)s, %(indicadordeforadoprazo)s, %(unidadesaláriocódigo)s, %(valorsaláriofixo)s)'''
            #for reg in dados:
            #sql = "insert into teste(nome, valor) values(%(nome)s, %(valor)s)"    
                cur.executemany(sql, dados)
            
        con.commit()
    except:
        with open('erro.json', mode='w') as arq:
            json.dump(dados, arq, indent=4)
        traceback.print_exc()
        exit()


def copiando_registros(arquivo, nome):
    print("COPIANDO ", nome)
    try:
        with postgres.connect(database="caged",
                        host="localhost",
                        #host="macbeth",
                        user="postgres",
                        password="",
                        port="5432") as con:
            with con.cursor() as cur:
                if "CAGEDEXC" in nome:
                    sql = f"COPY movimentacao_exc(linhas, competenciamov, regiao, uf, municipio, secao, subclasse, saldomovimentacao, cbo2002ocupacao, categoria, graudeinstrucao, idade, horascontratuais, racacor, sexo, tipoempregador, tipoestabelecimento, tipomovimentacao, tipodedeficiencia, indtrabintermitente, indtrabparcial, salario, tamestabjan, indicadoraprendiz, origemdainformacao, competenciadec, competenciaexc, indicadordeexclusao, indicadordeforadoprazo, unidadesalariocodigo, valorsalariofixo, arquivo, arg_hash, fxetaria) FROM STDIN WITH CSV HEADER"
                else:
                    sql = f"COPY movimentacao(linhas, competenciamov, regiao, uf, municipio, secao, subclasse, saldomovimentacao, cbo2002ocupacao, categoria, graudeinstrucao, idade, horascontratuais, racacor, sexo, tipoempregador, tipoestabelecimento, tipomovimentacao, tipodedeficiencia, indtrabintermitente, indtrabparcial, salario, tamestabjan, indicadoraprendiz, origemdainformacao, competenciadec, indicadordeforadoprazo, unidadesalariocodigo, valorsalariofixo, arquivo, arg_hash, fxetaria) FROM STDIN WITH CSV HEADER"
                cur.copy_expert(sql, arquivo)
    except Exception as erro:
        print(erro)
        traceback.print_exc()
        exit()


def calcula_hash(row) -> str:
    rotulos = ['competênciamov','região','uf','município','seção','subclasse','saldomovimentação','cbo2002ocupação','categoria','graudeinstrução','idade','horascontratuais','raçacor','sexo','tipoempregador','tipoestabelecimento','tipomovimentação','tipodedeficiência','indtrabintermitente','indtrabparcial','salário','tamestabjan','indicadoraprendiz','origemdainformação','competênciadec','indicadordeforadoprazo','unidadesaláriocódigo','valorsaláriofixo']
    valor = ';'.join(str(row[rot]) for rot in rotulos)
    return hashlib.md5(valor.encode()).hexdigest()

def hash_reg(row):
    rotulos = ['competênciamov','região','uf','município','seção','subclasse','saldomovimentação','cbo2002ocupação','categoria','graudeinstrução','idade','horascontratuais','raçacor','sexo','tipoempregador','tipoestabelecimento','tipomovimentação','tipodedeficiência','indtrabintermitente','indtrabparcial','salário','tamestabjan','indicadoraprendiz','origemdainformação','competênciadec','indicadordeforadoprazo','unidadesaláriocódigo','valorsaláriofixo']
    valor = ";".join(str(row[rot]) for rot in rotulos)
    return hashlib.md5(valor.encode()).hexdigest()

def calcula_faixaetaria(valor: str) -> int:
    if valor == None or len(valor) == 0:
        return 0
    x = float(valor.replace(',', '.'))
    if x <= 17:
        return 1
    elif x <= 24:
        return 2
    elif x <= 29:
        return 3
    elif x <= 39:
        return 4
    elif x <= 49:
        return 5
    elif x <= 64:
        return 6
    else:
        return 7

def converte(obj) -> float:
    if isinstance(obj, str):
        return float(obj.replace(',', '.'))
    else:
        return float(obj)

def remove_zeros(valor: str) -> str:
    valor = valor.strip()
    if len(valor) == 0:
        return '0'
    if valor[0] == ',':
        valor = '0' + valor
    if '.' in valor:
        aux = float(valor)
        valor = '{:.2f}'.format(aux)
        valor = valor.replace('.', ',')
    if valor.endswith(',00'):
        retorno = valor.replace(',00', '')
        if len(retorno) == 0:
            return '0'
        else:
            return retorno
    elif valor.endswith(',0'):
        retorno = valor.replace(',0', '')
        if len(retorno) == 0:
            return '0'
        else:
            return retorno
    else:
        return valor

def arruma_registros(dataframe, nome_arq):
    dataframe['arquivo'] = nome_arq

#    if not 'competênciaexc' in dataframe.columns:
#        dataframe.insert(25, 'competênciaexc', 0)
        
#    if not 'indicadordeexclusão' in dataframe.columns:
#        dataframe.insert(26, 'indicadordeexclusão', -1)

    dataframe['região'] = dataframe['região'].apply(lambda x: 9 if x == 99 else x)
    campos = ['horascontratuais', 'salário', 'valorsaláriofixo']    
    for campo in campos:
        dataframe[campo] = dataframe[campo].apply(lambda x: remove_zeros(x))


    dataframe['arg_hash'] = ";" + dataframe['competênciamov'].map(str) + ";" + dataframe['região'].map(str)  + ";" + dataframe['uf'].map(str)  + ";" + dataframe['município'].map(str) + ";" + dataframe['seção'].map(str) + ";" + dataframe['subclasse'].map(str) + ";" + dataframe['saldomovimentação'].map(str) + ";" + dataframe['cbo2002ocupação'].map(str) + ";" + dataframe['categoria'].map(str) + ";" + dataframe['graudeinstrução'].map(str) + ";" + dataframe['idade'].map(str) + ";" + dataframe['horascontratuais'].map(str) + ";" + dataframe['raçacor'].map(str) + ";" + dataframe['sexo'].map(str) + ";" + dataframe['tipoempregador'].map(str) + ";" + dataframe['tipoestabelecimento'].map(str) + ";" + dataframe['tipomovimentação'].map(str) + ";" + dataframe['tipodedeficiência'].map(str) + ";" + dataframe['indtrabintermitente'].map(str) + ";" + dataframe['indtrabparcial'].map(str) + ";" + dataframe['salário'].map(str) + ";" + dataframe['tamestabjan'].map(str) + ";" + dataframe['indicadoraprendiz'].map(str) + ";" + dataframe['origemdainformação'].map(str) + ";" + dataframe['competênciadec'].map(str) + ";" + dataframe['indicadordeforadoprazo'].map(str)  + ";" + dataframe['unidadesaláriocódigo'].map(str) + ";" + dataframe['valorsaláriofixo'].map(str) 


    dataframe['fxetaria'] = dataframe['idade'].apply(lambda x: calcula_faixaetaria(x))
    #Codigo para calcular o hash no python, mas o processo fica muito lento
    #dataframe['hash'] = hashlib.md5(';'.join(str(dataframe[rot]) for rot in rotulos).encode).hexdigest()


#def list_comp(df):
#    return pd.Series([hash_reg(row) for i, row in df.iterrows()])

pastas = os.listdir('dados')
try:
    for dir in pastas:
        if os.path.isdir(f'dados/{dir}'):
            arquivos = os.listdir(f'dados/{dir}')
            for nome_arq in arquivos:
                filename_path = f"dados/{dir}/{nome_arq}"
                print(f'Processando {filename_path}')
                ar = py7zr.SevenZipFile(filename_path)
                for name, fd in ar.read(name for name in ar.getnames() if name.endswith(".txt")).items():
                    ini = time.time()
                    dfs = pd.read_csv(fd, delimiter=';', converters={'salário' : str, 'valorsaláriofixo': str, 'horascontratuais': str, 'idade': str})
                    print(f"Leitura DF {time.time() - ini}")

                    ini = time.time()
                    arruma_registros(dfs, nome_arq)
                    ini = time.time()

                    print(f"Arrumando DF Panda  {time.time() - ini}")

                    ini = time.time()
                    dfs.to_csv(f"dados/{nome_arq}.csv")
                    print(f"Gravando CSV {time.time() - ini}")

                    ini = time.time()
                    file = open(f"dados/{nome_arq}.csv", mode="r")
                    copiando_registros(file, nome_arq)
                    os.remove(f"dados/{nome_arq}.csv")
                    print(f"Copiando Postgres {time.time() - ini}")
                    fd.close()
                    del dfs
                    gc.collect()
                ar.close()
except Exception:
    traceback.print_exc()
