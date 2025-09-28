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
nome_bucket_client='s3-client-venuste'
nome_bucket_trusted='s3-trusted-venuste'

client = boto3.client('s3', region_name=regiao)
s3 = boto3.resource('s3', region_name=regiao)

# ====================== #
#       Functions        #
# ====================== # 

urls_padrao=[
    'https://s3-trusted-venuste.s3.us-east-1.amazonaws.com/reclamacoes202407.csv',
    'https://s3-trusted-venuste.s3.us-east-1.amazonaws.com/reclamacoes202408.csv',
    'https://s3-trusted-venuste.s3.us-east-1.amazonaws.com/reclamacoes202409.csv',
    'https://s3-trusted-venuste.s3.us-east-1.amazonaws.com/reclamacoes202410.csv',
    'https://s3-trusted-venuste.s3.us-east-1.amazonaws.com/reclamacoes202411.csv',
    'https://s3-trusted-venuste.s3.us-east-1.amazonaws.com/reclamacoes202412.csv'
]

def refine_dados_consumidor_and_empresas(urls=urls_padrao ,uploadBucket=False):
    
    df_consumidor=pd.concat(
        map(pd.read_csv, urls), ignore_index=True)

    
    




# refine_dados_consumidor_and_empresas()