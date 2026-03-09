"""
Coletor CAUC — CKAN Tesouro Transparente.
Responsabilidade: baixar o relatório nacional, filtrar para municípios PB
e salvar os dados BRUTOS (sem classificação de pendências).

A classificação de pendências por gravidade é feita por:
    src/processors/cauc_processor.py

Rodar individualmente:
    python src/collectors/cauc.py
"""

import pandas as pd
import requests
import io
from pathlib import Path
from datetime import date
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Diretórios ─────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR  = BASE_DIR / "data" / "raw" / "cauc"
RAW_DIR.mkdir(parents=True, exist_ok=True)

TABELA = BASE_DIR / "data" / "processed" / "municipios_pb_tabela.csv"
HOJE   = date.today().strftime("%Y-%m-%d")

URL_CAUC_BULK = (
    "https://www.tesourotransparente.gov.br/ckan/dataset/"
    "72b5f371-0c35-4613-8076-c99c821a6410/resource/"
    "07af297a-5e59-494a-a88a-55ddfd2f4b01/download/"
    "relatorio-situacao-de-varios-entes---municipios---uf-todas---abrangencia-1.csv"
)


def run() -> pd.DataFrame:
    """
    Baixa o CSV nacional do CAUC, filtra municípios PB e salva raw.

    Outputs:
        raw/cauc/cauc_raw_pb_{HOJE}.csv  — snapshot datado
        raw/cauc/cauc_raw_pb.csv         — latest (sobrescrito a cada coleta)

    Retorna o DataFrame bruto filtrado (todas as colunas originais do CKAN
    + data_pesquisa + data_coleta).
    """
    print("=" * 70)
    print("  Coletor CAUC — CKAN Tesouro Transparente")
    print(f"  Execução: {HOJE}")
    print("=" * 70)

    # ── Carrega malha de municípios PB ────────────────────────────────────────
    municipios_df = pd.read_csv(TABELA, dtype={"cod_ibge": str})
    ibges_pb      = set(municipios_df["cod_ibge"].tolist())

    # ── Download nacional ─────────────────────────────────────────────────────
    print(f"\n  Baixando CSV nacional do CKAN...")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    resp    = requests.get(URL_CAUC_BULK, headers=headers, verify=False, timeout=60)
    resp.raise_for_status()

    # ── Tratamento de encoding ────────────────────────────────────────────────
    try:
        texto = resp.content.decode("utf-8-sig")
    except UnicodeDecodeError:
        texto = resp.content.decode("iso-8859-1")

    # Primeiras 3 linhas são metadados do Tesouro — skiprows=3
    df_raw = pd.read_csv(io.StringIO(texto), sep=";", skiprows=3,
                         dtype=str, na_filter=False)
    print(f"  ✅ {len(df_raw)} municípios no Brasil")

    # ── Extrai data da pesquisa do cabeçalho ──────────────────────────────────
    linhas        = texto.split("\n")
    data_pesquisa = linhas[0].strip().replace('"', '').replace('Data da Pesquisa: ', '')
    print(f"  Data da pesquisa: {data_pesquisa}")

    # ── Filtragem para Paraíba ────────────────────────────────────────────────
    col_ibge = next((c for c in df_raw.columns if "ibge" in c.lower()), None)
    if not col_ibge:
        raise ValueError(f"Coluna IBGE não encontrada. Colunas: {list(df_raw.columns)}")

    df_raw[col_ibge] = df_raw[col_ibge].astype(str)
    df_pb            = df_raw[df_raw[col_ibge].isin(ibges_pb)].copy()

    # Anexa metadados de coleta sem derivar nada dos dados fiscais
    df_pb["data_pesquisa"] = data_pesquisa
    df_pb["data_coleta"]   = HOJE

    print(f"  Municípios PB encontrados: {len(df_pb)}")

    # ── Exportação raw ────────────────────────────────────────────────────────
    df_pb.to_csv(RAW_DIR / f"cauc_raw_pb_{HOJE}.csv", index=False, encoding="utf-8-sig")
    df_pb.to_csv(RAW_DIR / "cauc_raw_pb.csv",          index=False, encoding="utf-8-sig")
    print(f"  Salvo em: raw/cauc/cauc_raw_pb.csv")
    print("=" * 70)

    return df_pb


if __name__ == "__main__":
    run()
