import pandas as pd
from thefuzz import fuzz
import re
import boto3
import botocore

# Config AWS
regiao='us-east-1'
bucket_raw='bucket-raw-g3'
bucket_trusted = "bucket-trusted-g3"
bucket_client='bucket-client-g3'
client = boto3.client('s3', region_name=regiao)
s3 = boto3.resource('s3', region_name=regiao)

# Empresas aéreas
client.download_file(bucket_trusted, "Empresas_Aereas_Tratado.xlsx", "Empresas_Aereas_Tratado.xlsx")
csv_empresas = "Empresas_Aereas_Tratado.xlsx"
empresas = pd.read_excel(csv_empresas) 
empresas = empresas[empresas["Tipo Empresa"] == "Nacional"]
empresas = empresas[['Razão Social', 'ICAO', 'País Sede', 'Cidade', 'UF', 'ID Empresa Aerea']]

# Dados consumidor
client.download_file(bucket_raw, "dadosconsumidor2024.csv", "dadosconsumidor2024.csv")
dados_consumidor = "dadosconsumidor2024.csv"
consumidor = pd.read_csv(dados_consumidor, sep=";", skiprows=1, usecols=[
        "Cidade",
        "Ano Abertura",
        "Mês Abertura",
        "Data e Hora Resposta",
        "Data Finalização",
        "Prazo Resposta",
        "Tempo Resposta (em dias)",
        "Nome Fantasia",
        "Assunto",
        "Grupo Problema",
        "Problema",
        "Forma Contrato",
        "Procurou Empresa",
        "Respondida",
        "Situação",
        "Avaliação Reclamação",
        "Nota do Consumidor"
    ])
consumidor = consumidor[consumidor['Mês Abertura'] > 6] # Pega apenas os últimos 6 meses

# Função que cria o mapeamento de Razão Social e Nome Fantasia
def criar_mapa_empresas(empresas, consumidor):
    consumidor["Nome Fantasia"] = [re.sub(r"Latam Airlines", "Tam", item) for item in consumidor["Nome Fantasia"]]
    consumidor["Nome Fantasia"] = [re.sub(r"Voepass Linhas Aéreas", "Passaredo", item) for item in consumidor["Nome Fantasia"]]

    list_empresas = empresas["Razão Social"].unique()
    list_consumidor = consumidor["Nome Fantasia"].unique()

    matches = {}
    for item1 in list_consumidor:
        best_match_score = 0
        best_match_item = None
        for item2 in list_empresas:
            score = fuzz.token_set_ratio(item1, item2)
            if score > best_match_score:
                best_match_score = score
                best_match_item = item2
        if (best_match_score > 90):
            matches.update([(item1, best_match_item)])

    return matches

# Função que junta as tabelas, utilizando o mapa da função anterior
def juntar_tabelas(mapa, df1, df2):
    df1.rename(columns={"Nome Fantasia":"Razão Social"}, inplace=True)
    df1["Razão Social"] = df1["Razão Social"].map(mapa)
    df3 = pd.merge(df1, df2, on="Razão Social", how="inner")
    tabelao = "tabelao_reclamacoes_empresas.csv"
    output = df3.to_csv(tabelao, index=False, sep=";")
    return tabelao

# MAIN
mapa = criar_mapa_empresas(empresas, consumidor)
tabelao = juntar_tabelas(mapa, consumidor, empresas)
client.upload_file(tabelao, bucket_client, tabelao)