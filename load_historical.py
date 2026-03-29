"""
load_historical.py — Carga histórica única para o BigQuery.
Lê os arquivos já coletados em disco e sobe tudo de uma vez.

Uso normal:
    python load_historical.py --uf PB

Uso para re-upload de tabela específica (ex: após fix de schema):
    python load_historical.py --uf PB --tabela cauc
    python load_historical.py --uf PB --tabela pncp

O primeiro chunk de cada tabela usa write_mode="replace" (trunca a tabela no BQ
e reinicia do zero). Os chunks seguintes usam "append". Isso garante que um
re-upload completo sempre substitui os dados anteriores, sem duplicação.
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.utils.bigquery_loader import upload_raw
from src.collectors.cauc import CAUC_COL_MAP

CHUNK = 50_000


def carregar_csv(
    path:    Path,
    uf:      str,
    table:   str,
    col_map: dict | None = None,
) -> None:
    """
    Carrega CSV em chunks e faz upload para o BigQuery.

    col_map : dict de rename de colunas aplicado a cada chunk antes do upload.
              Não altera o arquivo em disco. Usado para o CAUC, cujas colunas
              originais ("1.1", "1.2" etc.) precisam de nomes semânticos no BQ.
    """
    print(f"\n  📂 {path.name} → raw.{table}")
    total = 0
    for i, chunk in enumerate(pd.read_csv(path, dtype=str, chunksize=CHUNK)):
        if col_map:
            chunk = chunk.rename(columns=col_map)
        mode = "replace" if i == 0 else "append"
        upload_raw(chunk, table, uf, write_mode=mode)
        total += len(chunk)
        print(f"     chunk {i+1}: {total:,} linhas enviadas", end="\r")
    print(f"     ✅ {total:,} linhas totais → raw.{table}            ")


def carregar_jsonl(path: Path, uf: str, table: str) -> None:
    """
    Carrega JSONL em chunks e faz upload para o BigQuery.

    Dicts aninhados (ex: orgaoEntidade, unidadeOrgao do PNCP) são
    serializados para JSON válido pelo _sanitize() do bigquery_loader,
    que agora chama json.dumps() antes de astype(str).
    """
    print(f"\n  📂 {path.name} → raw.{table}")
    buffer = []
    total  = 0
    first  = True

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                buffer.append(json.loads(line))
            except json.JSONDecodeError:
                continue

            if len(buffer) >= CHUNK:
                df   = pd.DataFrame(buffer)
                mode = "replace" if first else "append"
                upload_raw(df, table, uf, write_mode=mode)
                total += len(df)
                first  = False
                buffer = []
                print(f"     {total:,} linhas enviadas...", end="\r")

    if buffer:
        df   = pd.DataFrame(buffer)
        mode = "replace" if first else "append"
        upload_raw(df, table, uf, write_mode=mode)
        total += len(df)

    print(f"     ✅ {total:,} linhas totais → raw.{table}            ")


# Catálogo completo de cargas por UF.
# Cada entrada: (tipo, path_relativo_ao_raw, nome_tabela_bq, col_map_ou_None)
def _build_cargas(raw: Path, uf: str) -> list[tuple]:
    uf_lower = uf.lower()
    return [
        ("csv",   raw / "cauc"    / uf / f"cauc_raw_{uf_lower}.csv",       "cauc",        CAUC_COL_MAP),
        ("csv",   raw / "dca"     / uf / f"dca_raw_{uf_lower}.csv",        "dca",         None),
        ("csv",   raw / "siconfi" / uf / f"siconfi_rreo_{uf_lower}.csv",   "siconfi_rreo",None),
        ("csv",   raw / "siconfi" / uf / f"siconfi_rgf_{uf_lower}.csv",    "siconfi_rgf", None),
        ("jsonl", raw / "pncp"    / uf / f"pncp_parcial_{uf_lower}.jsonl", "pncp",        None),
    ]


def main():
    parser = argparse.ArgumentParser(
        description="Carga histórica de arquivos raw para o BigQuery."
    )
    parser.add_argument("--uf",     default="PB",
                        help="Sigla do estado (default: PB)")
    parser.add_argument("--tabela", default=None,
                        help="Se informado, carrega apenas essa tabela. "
                             "Opções: cauc | dca | siconfi_rreo | siconfi_rgf | pncp")
    args = parser.parse_args()
    uf   = args.uf.upper()

    raw    = ROOT / "data" / "raw"
    cargas = _build_cargas(raw, uf)

    if args.tabela:
        cargas = [c for c in cargas if c[2] == args.tabela]
        if not cargas:
            print(f"  ⚠️  Tabela '{args.tabela}' não reconhecida. "
                  f"Opções: cauc, dca, siconfi_rreo, siconfi_rgf, pncp")
            sys.exit(1)

    print(f"\n{'='*55}")
    print(f"  CARGA HISTÓRICA → BigQuery raw | UF: {uf}")
    if args.tabela:
        print(f"  Tabela filtrada : {args.tabela}")
    print(f"{'='*55}")

    for entrada in cargas:
        tipo, path, table, col_map = entrada
        if not path.exists():
            print(f"\n  ⚠️  Não encontrado: {path} — pulando.")
            continue
        if tipo == "csv":
            carregar_csv(path, uf, table, col_map=col_map)
        else:
            carregar_jsonl(path, uf, table)

    print(f"\n{'='*55}")
    print("  ✅ Carga histórica concluída.")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
