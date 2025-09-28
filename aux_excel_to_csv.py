import pandas as pd
filePath='Empresas_Aereas_Tratado.xlsx'

df=pd.read_excel(filePath)
df.to_csv("empresas_aereas_tratado.csv")