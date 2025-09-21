import boto3
import botocore 
import pandas as pd
import re
import math

# ======== Configuração AWS =======

# Obs>>>>>>>>>> não esquecer de configurar as credenciais AWS no ambiente local

regiao='us-east-1'
nome_bucket_raw='bucketz-0001'
nome_bucket_trusted='bucketz-0002'

client = boto3.client('s3', region_name=regiao)
s3 = boto3.resource('s3', region_name=regiao)

# ======== Funções Bucket ========

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
    """
    Deleta um arquivo do bucket caso exista.
    :param bucketName: Nome do bucket S3
    :param filePath: Caminho/arquivo dentro do bucket
    """
    try:  
        s3.Object(bucketName, filePath).load()
    except botocore.exceptions.ClientError as erro:
        if erro.response['Error']['Code'] == '404':
            return 'arquivo não existe'
        else: 
            raise erro
    else:
        s3.Object(bucketName, filePath).delete()
        return 'arquivo deletado'
    
def subir_arquivo_deletando_se_existe(bucketName, filePath, fileNameOnBucket):
    """
    Sobe um arquivo para o bucket. 
    Se já existir, deleta antes.
    :param bucketName: Nome do bucket S3
    :param filePath: Caminho local do arquivo
    :param fileNameOnBucket: Nome/caminho do arquivo no bucket
    """
    deletar_arquivo_se_existe(bucketName, fileNameOnBucket)
    client.upload_file(filePath, bucketName, fileNameOnBucket)
    print(f"Arquivo '{filePath}' enviado para '{bucketName}/{fileNameOnBucket}'")

# === Baixar e ler arquivo do bucket RAW === 

raw_file_s3 = "aerodromospublicos.xls"   # caminho no bucket raw
base_de_dados = "aerodromospublicos.xls" # nome local após download

baixar_arquivo(nome_bucket_raw, raw_file_s3, base_de_dados)

# Ler o Excel
df = pd.read_excel(base_de_dados, header=2, engine="xlrd")

print(df.columns)

# === Pipeline ===
def dms_para_dd(graus, minutos, segundos, direcao):
    decimal = graus + (minutos / 60) + (segundos / 3600)
    if direcao in ['S', 'W']:
        decimal = -decimal
    return decimal

# Função para parsear a string "8° 20' 55'' S"
def parse_dms(dms_str):
    # Regex para capturar graus, minutos, segundos e direção
    padrao = r"(\d+)°\s*(\d+)'?\s*(\d+)''?\s*([NSEW])"
    match = re.match(padrao, dms_str.strip())
    if not match:
        return None
    graus, minutos, segundos, direcao = match.groups()
    return dms_para_dd(int(graus), int(minutos), int(segundos), direcao)

# Converter colunas de latitude e longitude
df["LAT_DD"] = df["LATITUDE"].apply(parse_dms)
df["LON_DD"] = df["LONGITUDE"].apply(parse_dms)

# Salvar resultado final
output_file = "aerodromodpublicos_tratado.xlsx"
df.to_excel(output_file, index=False)

# Salva arquivo tratado localmente
output_file

# Envia para o bucket trusted
subir_arquivo_deletando_se_existe(
    nome_bucket_trusted,
    output_file,
    "aerodromospublicos_tratado.xlsx"
)
