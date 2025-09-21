import boto3
import botocore # erros de boto3 para exceptions (404, ver se existe)
import os
import pandas as pd
import dask.dataframe as dd # utilizado para leitura de arquivos massivos
import unicodedata # utilizado para limpeza de caracteres especiais
import re # utilizado para limpeza de caracteres especiais

# ====================== #
#        Config          #
# ====================== # 

regiao='us-east-1'
nome_bucket_raw='s3-raw-venuste'
nome_bucket_trusted='s3-trusted-venuste'

client = boto3.client('s3', region_name=regiao)
s3 = boto3.resource('s3', region_name=regiao)

mapa = {
'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a',
'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o',
'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
'ç': 'c',
'Á': 'A', 'À': 'A', 'Â': 'A', 'Ã': 'A', 'Ä': 'A',
'É': 'E', 'È': 'E', 'Ê': 'E', 'Ë': 'E',
'Í': 'I', 'Ì': 'I', 'Î': 'I', 'Ï': 'I',
'Ó': 'O', 'Ò': 'O', 'Ô': 'O', 'Õ': 'O', 'Ö': 'O',
'Ú': 'U', 'Ù': 'U', 'Û': 'U', 'Ü': 'U',
'Ç': 'C'
}

# ====================== #
#       Functions        #
# ====================== # 

def deletar_arquivo_se_existe(bucketName, filePath):
    try:  
        s3.Object(bucketName, filePath).load()
    except botocore.exceptions.ClientError as erro:
        # Arquivo não existe
        if erro.response['Error']['Code'] == '404':
            return 'arquivo não existe'
        else: 
        # Erro inesperado
            raise erro
    else:
        # Deletando arquivo (mais performático, eu acho)
        s3.Object(bucketName, filePath).delete()
        return 'arquivo deletado'
    
def subir_arquivo_deletando_se_existe(bucketName, filePath, fileNameOnBucket):
    deletar_arquivo_se_existe(bucketName, filePath)
    client.upload_file(filePath ,bucketName, fileNameOnBucket)

def clean_dadosconsumidor2024(mes, filePath="dados_atividade/dadosconsumidor2024.csv", uploadBucket=False):

    dadosConsumidorDtype={
    'Data e Hora Análise': 'object',
    'Data e Hora Recusa': 'object',
    'Tempo Resposta (em dias)': 'float64',
    'Data e Hora Resposta': 'object'
    }

    dados_consumidor = pd.read_csv(filePath, 
                                   sep=';', 
                                   dtype=dadosConsumidorDtype, 
                                   parse_dates=['Data e Hora Resposta'],
                                   date_format=dd.to_datetime
                                )
    dados_consumidor=dados_consumidor[dados_consumidor['Mês Abertura'] == 6]

    dados_consumidor['Data Abertura']=dados_consumidor['Data Abertura'].str[8:]

    dados_consumidor=dados_consumidor.replace(mapa, regex=True)
    
    fileName = ''
    if (mes < 10):
        fileName = 'reclamacoes20240' + str(mes) + '.csv'  
    else:
        fileName = 'reclamacoes2024' + str(mes) + '.csv'

    dados_consumidor.to_csv(fileName)

    if (uploadBucket):
        subir_arquivo_deletando_se_existe(nome_bucket_trusted, fileName, fileName)




def limpar_arquivos_locais(nome):
    if os.path.exists(nome):
        os.remove(nome)