import boto3
import botocore # erros de boto3 para exceptions (404, ver se existe)
import os
import pandas as pd
import dask.dataframe as dd # utilizado para leitura de arquivos massivos

# ====================== #
#        Config          #
# ====================== # 

regiao='us-east-1'
nome_bucket_raw=''
nome_bucket_trusted=''

client = boto3.client('s3', region_name=regiao)
s3 = boto3.resource('s3', region_name=regiao)

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

def clean_dadosconsumidor2024():

    def criar_subset_limpo(campoData, dataInicio, dataFim):

        return 0
        
    dadosConsumidorDtype={
    'Data e Hora Análise': 'object',
    'Data e Hora Recusa': 'object',
    'Tempo Resposta (em dias)': 'float64'
    }

    dados_consumidor = dd.read_csv("dados_atividade/dadosconsumidor2024.csv", 
                                   sep=';', 
                                   dtype=dadosConsumidorDtype, 
                                   parse_dates=['Data e Hora Resposta'],
                                   date_format=dd.to_datetime
                                )

    print(dados_consumidor.head()['Data e Hora Resposta'])
    df_2024_01=dados_consumidor[dados_consumidor['Data e Hora Resposta'] < '2024-02-01 00:00:00']
    df_2024_02=dados_consumidor[(dados_consumidor['Data e Hora Resposta'] > '2024-01-31 23:59:59') & (dados_consumidor['Data e Hora Resposta'] < '2024-03-01 00:00:00')]
    df_2024_03=dados_consumidor[(dados_consumidor['Data e Hora Resposta'] > '2024-02-29 23:59:59') & (dados_consumidor['Data e Hora Resposta'] < '2024-04-01 00:00:00')]
    df_2024_04=dados_consumidor[(dados_consumidor['Data e Hora Resposta'] > '2024-03-31 23:59:59') & (dados_consumidor['Data e Hora Resposta'] < '2024-05-01 00:00:00')]
    df_2024_05=dados_consumidor[(dados_consumidor['Data e Hora Resposta'] > '2024-04-30 23:59:59') & (dados_consumidor['Data e Hora Resposta'] < '2024-06-01 00:00:00')]
    df_2024_06=dados_consumidor[(dados_consumidor['Data e Hora Resposta'] > '2024-05-31 23:59:59') & (dados_consumidor['Data e Hora Resposta'] < '2024-07-01 00:00:00')]
    df_2024_07=dados_consumidor[(dados_consumidor['Data e Hora Resposta'] > '2024-06-30 23:59:59') & (dados_consumidor['Data e Hora Resposta'] < '2024-08-01 00:00:00')]
    df_2024_08=dados_consumidor[(dados_consumidor['Data e Hora Resposta'] > '2024-07-31 23:59:59') & (dados_consumidor['Data e Hora Resposta'] < '2024-09-01 00:00:00')]
    df_2024_09=dados_consumidor[(dados_consumidor['Data e Hora Resposta'] > '2024-08-31 23:59:59') & (dados_consumidor['Data e Hora Resposta'] < '2024-10-01 00:00:00')]
    df_2024_10=dados_consumidor[(dados_consumidor['Data e Hora Resposta'] > '2024-09-30 23:59:59') & (dados_consumidor['Data e Hora Resposta'] < '2024-11-01 00:00:00')]
    df_2024_11=dados_consumidor[(dados_consumidor['Data e Hora Resposta'] > '2024-10-31 23:59:59') & (dados_consumidor['Data e Hora Resposta'] < '2024-12-01 00:00:00')]
    df_2024_12=dados_consumidor[(dados_consumidor['Data e Hora Resposta'] > '2024-11-30 23:59:59') & (dados_consumidor['Data e Hora Resposta'] < '2025-01-01 00:00:00')]
    print(len(dados_consumidor))
    print(len(df_2024_01)+ len(df_2024_02)+ len(df_2024_03) + len(df_2024_04) + len(df_2024_05) + len(df_2024_06) + len(df_2024_07) + len(df_2024_08) + len(df_2024_09) + len(df_2024_10) + len(df_2024_11) + len(df_2024_12))
clean_dadosconsumidor2024()



def limpar_arquivos_locais(nome):
    if os.path.exists(nome):
        os.remove(nome)