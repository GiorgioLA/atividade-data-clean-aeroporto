import boto3
import botocore # erros de boto3 para exceptions (404, ver se existe)
import pandas as pd
import re

# ======== Configuração AWS =======

# Obs>>>>>>>>>> não esquecer de configurar as credenciais AWS no ambiente local

regiao='us-east-1'
nome_bucket_raw='bucketz-0001'
nome_bucket_trusted='bucketz-0002'

client = boto3.client('s3', region_name=regiao)
s3 = boto3.resource('s3', region_name=regiao)

# ======== Funções Bucket =======

def baixar_arquivo(bucketName, fileNameOnBucket, localPath):
    """
    Baixa um arquivo do S3 e salva localmente.
    :param bucketName: Nome do bucket S3
    :param fileNameOnBucket: Caminho/arquivo dentro do bucket
    :param localPath: Caminho onde salvar localmente
    """
    try:
        client.download_file(bucketName, fileNameOnBucket, localPath)
        print(f"Arquivo '{fileNameOnBucket}' baixado de '{bucketName}' para '{localPath}'")
    except botocore.exceptions.ClientError as erro:
        if erro.response['Error']['Code'] == "404":
            print("Arquivo não encontrado no bucket")
        else:
            raise erro

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

# Funções

def salvar_csv(df, nome_csv):
    output_file = nome_csv
    df.to_csv(output_file, index=False)
    output_file

def separar_dataframe_por_mes(df, lista_index):
    mes = 1
    for i in range(len(lista_index)):
        inicio = lista_index[i]
        fim = lista_index[i+1] if i < len(lista_index)-1 else None
        novo_df = df.iloc[inicio:fim, :] if fim != None else df.iloc[inicio:, :]

        salvar_csv(novo_df, f"VRA_2024_{mes:02}.csv")
        mes += 1

# === Baixar e ler arquivo do bucket RAW === 

raw_file_s3 = "Empresas Aereas.xlsx"   # caminho no bucket raw
base_de_dados = "VRA_2024.csv"     # nome local após download

# baixar_arquivo(nome_bucket_raw, raw_file_s3, base_de_dados)
index_headers_repetidos = [0, 86610, 163842, 245317, 326015, 404897, 482977, 570728, 656095, 737544, 821837, 901865]
df = pd.read_csv(base_de_dados, sep=';') 

# === Pipeline ===

# Aplicar transformações
separar_dataframe_por_mes(df, index_headers_repetidos)

# Envia para o bucket trusted
# subir_arquivo_deletando_se_existe(
#     nome_bucket_trusted,
#     output_file,
#     "Empresas_Aereas_Tratado.xlsx"
# )