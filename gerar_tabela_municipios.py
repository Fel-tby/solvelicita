import httpx
import pandas as pd
from pathlib import Path

OUT = Path("data/processed/municipios_pb_tabela.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

print("Buscando municípios da PB no SICONFI...")
r = httpx.get("https://apidatalake.tesouro.gov.br/ords/siconfi/tt/entes", timeout=30)
todos = r.json().get("items", [])

pb = [e for e in todos if e.get("uf") == "PB" and e.get("esfera") == "M"]
df = pd.DataFrame(pb)
df.to_csv(OUT, index=False, encoding="utf-8")

print(f"✅ {len(df)} municípios salvos em {OUT}")
print(df[["cod_ibge", "ente", "cnpj", "populacao"]].head())
