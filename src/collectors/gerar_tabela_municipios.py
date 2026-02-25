"""
Script utilitário para geração da malha geográfica base (Backbone).
Consulta a API do SICONFI para extrair o cadastro oficial de municípios
e estruturar a dimensão primária utilizada por todos os coletores do projeto.
"""

import httpx
import pandas as pd
from pathlib import Path

# ── Configurações de Output ───────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
OUT = BASE_DIR / "data" / "processed" / "municipios_pb_tabela.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

print("Buscando municípios da PB no SICONFI...")

# ── Ingestão de Dados ─────────────────────────────────────────────────────────
r = httpx.get("https://apidatalake.tesouro.gov.br/ords/siconfi/tt/entes", timeout=30)
todos = r.json().get("items", [])

# ── Filtragem e Processamento ─────────────────────────────────────────────────
# Isola entidades de esfera Municipal ('M') restritas à Unidade Federativa 'PB'
pb = [e for e in todos if e.get("uf") == "PB" and e.get("esfera") == "M"]
df = pd.DataFrame(pb)

# ── Exportação ────────────────────────────────────────────────────────────────
df.to_csv(OUT, index=False, encoding="utf-8")

print(f"✅ {len(df)} municípios salvos em {OUT}")
print(df[["cod_ibge", "ente", "cnpj", "populacao"]].head())