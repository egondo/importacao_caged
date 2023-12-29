import ftplib
import os
import hashlib
#ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/ 

def md5sum(filename):
    md5 = hashlib.md5()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
            md5.update(chunk)
    return md5.hexdigest()

def arquivo_existe(nome1, nome2):
    if os.path.exists(nome1) or os.path.exists(nome2):
        return True
    else:
        return False
    
def apaga_duplicidade(nome_orig, nome_proc, nome_novo):
    if os.path.exists(nome_novo):
        md5_novo = md5sum(nome_novo)
        if os.path.exists(nome_orig):
            md5_valor = md5sum(nome_orig)
            if md5_novo == md5_valor:
                print("removendo arquivo", nome_novo)
                os.remove(nome_novo)
            else:
                os.remove(nome_orig)
        elif os.path.exists(nome_proc):
            md5_valor = md5sum(nome_proc)
            if md5_novo == md5_valor:
                print("removendo arquivo", nome_novo)
                os.remove(nome_novo)
            else:
                os.remove(nome_proc)
             

ftp = ftplib.FTP("ftp.mtps.gov.br")
ftp.login("anonymous", "ftplib-example-1")
ftp.encoding = 'ISO8859-1'
pasta_caged = "/pdet/microdados/NOVO CAGED/"
ftp.cwd(pasta_caged)
#anos = [2020, 2021, 2023]
anos = [2022]
for ano in anos:
    ftp.cwd(f'{ano}')
    data = ftp.nlst()
    print(data)
    for arq in data:
        if not arq.endswith('7z'):  
            ftp.cwd(arq)
            arquivos = ftp.nlst()
            if not os.path.exists(f'./dados/{arq}'):
                os.makedirs(f'./dados/{arq}')
            for caged in arquivos:
                nome_orig = f'./dados/{arq}/{caged}'
                nome_proc = f'./dados/{arq}/proc_{caged}'
                nome_novo = f'./dados/{arq}/novo_{caged}'
                if arquivo_existe(nome_orig, nome_proc):
                    file = open(nome_novo, "wb")
                else:
                    file = open(nome_orig, "wb")    
                
                ftp.retrbinary(f"RETR {caged}", file.write)
                file.close()

                apaga_duplicidade(nome_orig, nome_proc, nome_novo)
                
  

            ftp.cwd("../")  
    ftp.cwd(pasta_caged)
ftp.quit()
 

