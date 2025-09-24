import pandas as pd

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
raw_file2 = "Empresas_Aereas_Tratado.xlsx"
df2 = pd.read_excel(raw_file2, sheet_name="20250106") 
df2 = df2[df2["Tipo Empresa"] == "Nacional"]
df2 = df2[['Razão Social', 'ICAO', 'País Sede', 'Cidade', 'UF', 'ID Empresa Aerea']]
# output = df2.to_excel("Empresas_tratadas.xlsx", index=False)
# output

# === Tabelão ===
df2.rename(columns={"Razão Social": "Empresa Aérea"}, inplace=True)
df3 = pd.merge(df1, df2, on="Empresa Aérea", how="inner")
output = df3.to_csv("Tabelao.csv", index=False, sep=";")