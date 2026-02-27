#extract data mercado financeiro

import requests
import pandas as pd
from datetime import datetime

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados"

# Códigos das séries
SERIES = {
    "selic": 11,
    "ipca": 433,
    "dolar": 1
}

def fetch_series(code, start_date, end_date):
    params = {
        "formato": "json",
        "dataInicial": start_date,
        "dataFinal": end_date
    }
    
    url = BASE_URL.format(code)
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    df = pd.DataFrame(data)
    
    df["data"] = pd.to_datetime(df["data"], dayfirst=True)
    df["valor"] = pd.to_numeric(df["valor"])
    
    return df

start = "01/01/2020"
end = datetime.today().strftime("%d/%m/%Y")

dfs = {}

for name, code in SERIES.items():
    df = fetch_series(code, start, end)
    df = df.rename(columns={"valor": name})
    dfs[name] = df[["data", name]]

# Merge das séries por data
df_final = dfs["selic"]

for name in ["ipca", "dolar"]:
    df_final = df_final.merge(dfs[name], on="data", how="inner")

df_final = df_final.sort_values("data")

print(df_final.head())