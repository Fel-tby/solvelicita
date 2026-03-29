"""
Coletor de tabela base de municípios por UF (backbone geográfico).
Consulta a API do SICONFI e salva o cadastro oficial de municípios.
Pré-requisito para todos os demais coletores.

Rodar individualmente:
    python src/collectors/municipios.py
    python src/collectors/municipios.py --uf CE
"""

import sys
import httpx
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.paths import get_paths, PROCESSED
from utils.bigquery_loader import upload_raw


def run(uf: str = "PB") -> pd.DataFrame:
    """
    Busca municípios da UF informada no SICONFI e salva CSV de referência.
    Retorna DataFrame com colunas: cod_ibge, ente, cnpj, populacao, ...
    """
    uf    = uf.upper()
    paths = get_paths(uf)
    out   = paths["processed"] / f"municipios_{uf.lower()}_tabela.csv"

    print(f"Buscando municípios de {uf} no SICONFI...")
    r     = httpx.get(
        "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/entes", timeout=30
    )
    todos = r.json().get("items", [])

    municipios = [
        e for e in todos
        if e.get("uf") == uf and e.get("esfera") == "M"
    ]
    df = pd.DataFrame(municipios)

    # Primário — nova estrutura UF subfolder
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"✅ {len(df)} municípios salvos em {out}")

    # Legacy flat — processors leem daqui até o Bloco 9
    out_compat = PROCESSED.parent / f"municipios_{uf.lower()}_tabela.csv"
    df.to_csv(out_compat, index=False, encoding="utf-8")
    print(f"  [compat] {out_compat.name}: escrito para processors legados")

    print(df[["cod_ibge", "ente", "cnpj", "populacao"]].head())

    upload_raw(df, "dim_municipios", uf)
    return df


if __name__ == "__main__":
    uf_arg = "PB"
    args   = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--uf" and i + 1 < len(args):
            uf_arg = args[i + 1]
        elif arg.startswith("--uf="):
            uf_arg = arg.split("=", 1)[1]
    run(uf=uf_arg)