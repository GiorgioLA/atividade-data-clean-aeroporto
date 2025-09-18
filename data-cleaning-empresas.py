import boto3
import botocore # erros de boto3 para exceptions (404, ver se existe)
import pandas as pd
import re

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

# === Leitura do arquivo ===
base_de_dados ="Empresas Aereas.xlsx"
df = pd.read_excel(base_de_dados, sheet_name="20250106") 

# === Funções auxiliares ===

def padronizar_cnpj(cnpj):
    """Remove caracteres não numéricos e valida se tem 14 dígitos."""
    if pd.isna(cnpj):
        return None
    cnpj = re.sub(r"\D", "", str(cnpj))
    return cnpj if len(cnpj) == 14 else None

def padronizar_cep(cep):
    """Remove espaços extras no CEP."""
    if pd.isna(cep):
        return None
    return str(cep).replace(" ", "")

def padronizar_telefone(telefone):
    """Padroniza o formato do telefone: (XX) XXXXX-XXXX ou (XX) XXXX-XXXX"""
    if pd.isna(telefone):
        return None
    telefone = re.sub(r"\D", "", str(telefone))
    if len(telefone) == 11:
        return f"({telefone[:2]}) {telefone[2:7]}-{telefone[7:]}"
    elif len(telefone) == 10:
        return f"({telefone[:2]}) {telefone[2:6]}-{telefone[6:]}"
    return telefone  # retorna cru se não bater

def padronizar_email(email):
    """Transforma em minúsculas, remove espaços extras e permite lista separada por vírgula."""
    if pd.isna(email):
        return None
    emails = [e.strip().lower() for e in str(email).split(",")]
    return ", ".join(emails)

def padronizar_data(data):
    """Mantém vazio se não for data válida, caso contrário retorna yyyy-mm-dd como string."""
    if pd.isna(data) or str(data).strip() == "":
        return ""
    try:
        return pd.to_datetime(data).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return ""

def padronizar_endereco(endereco):
    """Transforma endereço para minúsculas e remove espaços extras."""
    if pd.isna(endereco):
        return None
    return str(endereco).strip().lower()

def classificar_empresa(pais):
    """Classifica empresa como Nacional ou Estrangeira com base no país de sede."""
    if pd.isna(pais):
        return "Desconhecido"
    pais = str(pais).strip().lower()
    if pais in ["brasil", "br"]:
        return "Nacional"
    return "Estrangeira"


# === Pipeline ===

# Aplicar transformações
df["CNPJ"] = df["CNPJ"].apply(padronizar_cnpj)
df = df[df["CNPJ"].notna()]  # remove inválidos

if "CEP" in df.columns:
    df["CEP"] = df["CEP"].apply(padronizar_cep)

if "Telefone" in df.columns:
    df["Telefone"] = df["Telefone"].apply(padronizar_telefone)

if "Email" in df.columns:
    df["Email"] = df["Email"].apply(padronizar_email)

if "Data Decisão Operacional" in df.columns:
    df["Data Decisão Operacional"] = df["Data Decisão Operacional"].apply(padronizar_data)

if "Validade Operacional" in df.columns:
    df["Validade Operacional"] = df["Validade Operacional"].apply(padronizar_data)

if "Endereço" in df.columns:
    df["Endereço"] = df["Endereço"].apply(padronizar_endereco)

if "País Sede" in df.columns:
    df["Tipo Empresa"] = df["País Sede"].apply(classificar_empresa)


# Salvar resultado final
output_file = "Empresas_Aereas_Tratado.xlsx"
df.to_excel(output_file, index=False)

# Salva arquivo tratado localmente
output_file

# Envia para o bucket trusted
subir_arquivo_deletando_se_existe(
    nome_bucket_trusted,
    output_file,
    "trusted/Empresas_Aereas_Tratado.xlsx"
)