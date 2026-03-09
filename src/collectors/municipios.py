"""
Coletor de tabela base de municípios da Paraíba (backbone geográfico).
Consulta a API do SICONFI e salva o cadastro oficial de municípios PB.
Pré-requisito para todos os demais coletores.

Rodar individualmente:
    python src/collectors/municipios.py
"""

import httpx
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
OUT      = BASE_DIR / "data" / "processed" / "municipios_pb_tabela.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)


def run() -> pd.DataFrame:
    """
    Busca municípios PB no SICONFI e salva CSV de referência.
    Retorna DataFrame com colunas: cod_ibge, ente, cnpj, populacao, ...
    """
    print("Buscando municípios da PB no SICONFI...")
    r    = httpx.get("https://apidatalake.tesouro.gov.br/ords/siconfi/tt/entes", timeout=30)
    todos = r.json().get("items", [])

    pb = [e for e in todos if e.get("uf") == "PB" and e.get("esfera") == "M"]
    df = pd.DataFrame(pb)

    df.to_csv(OUT, index=False, encoding="utf-8")
    print(f"✅ {len(df)} municípios salvos em {OUT}")
    print(df[["cod_ibge", "ente", "cnpj", "populacao"]].head())
    return df


if __name__ == "__main__":
    run()
