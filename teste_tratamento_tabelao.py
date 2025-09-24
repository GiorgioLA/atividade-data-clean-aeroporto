import pandas as pd
import boto3
import botocore
import re

# --------------- Configuranções da AWS ---------------

regiao='us-east-1'
bucket_raw = "teste-ad1-raw"
bucket_trusted = "teste-ad1-trusted"
nome_bucket_client='client-teste-ativ-tabelao'
client = boto3.client('s3', region_name=regiao)
s3 = boto3.resource('s3', region_name=regiao)

client.download_file(bucket_raw, "VRA_2024.csv", "VRA_2024.csv")
client.download_file(bucket_trusted, "Empresas_Aereas_Tratado.xlsx", "Empresas_Aereas_Tratado.xlsx")

# Voos
raw_file1 = "VRA_2024.csv"
df1 = pd.read_csv(raw_file1, sep=';', usecols=[
    "Sigla ICAO Empresa Aérea","Empresa Aérea",
    "Número Voo","Número de Assentos",
    "Sigla ICAO Aeroporto Origem","Descrição Aeroporto Origem",
    "Partida Real","Sigla ICAO Aeroporto Destino",
    "Descrição Aeroporto Destino","Chegada Real","Situação Voo"
    ]) 
df1 = df1.iloc[404897:, :]
df1 = df1[df1["Sigla ICAO Aeroporto Origem"].str.startswith("SB") &
        df1["Sigla ICAO Aeroporto Destino"].str.startswith("SB")]
# output = df1.to_csv("VRA_tratado.csv", index=False, sep=";")
# output

#Empresas
trusted_file2 = "Empresas_Aereas_Tratado.xlsx"
df2 = pd.read_excel(trusted_file2) 
df2 = df2[df2["Tipo Empresa"] == "Nacional"]
df2 = df2[['Razão Social', 'ICAO', 'País Sede', 'Cidade', 'UF', 'ID Empresa Aerea']]
# output = df2.to_excel("Empresas_tratadas.xlsx", index=False)
# output

# === Tabelão ===
df2.rename(columns={"Razão Social": "Empresa Aérea"}, inplace=True)
df3 = pd.merge(df1, df2, on="Empresa Aérea", how="inner")
output = df3.to_csv("Tabelao.csv", index=False, sep=";")


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
    client.upload_file("VRA_2024.csv", bucket_raw, "VRA_2024.csv")
    client.upload_file("Empresas_Aereas_Tratado.xlsx", bucket_raw, "Empresas_Aereas_Tratado.xlsx")
    print(f"Arquivo '{filePath}' enviado para '{bucketName}/{fileNameOnBucket}'")

subir_arquivo_deletando_se_existe(nome_bucket_client,
                                  "Tabelao.csv",
                                 "Tabelao.csv")  
