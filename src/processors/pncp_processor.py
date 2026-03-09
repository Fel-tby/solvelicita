"""
Processador PNCP — consolida e normaliza licitações PB.
Lê o JSONL produzido por src/collectors/pncp.py e realiza:
    1. Consolidação JSONL → DataFrame
    2. Flattening de campos aninhados (orgaoEntidade, unidadeOrgao)
    3. Seleção das colunas relevantes (KEEP)
    4. Exportação para processed/

Input:
    raw/pncp/pncp_parcial.jsonl   (produzido por collectors/pncp.py)

Output:
    processed/pncp_licitacoes_pb.csv   — base para pncp_agregador.py
    raw/pncp/pncp_snapshot_{HOJE}.csv  — snapshot datado

Rodar individualmente:
    python src/processors/pncp_processor.py
"""

import json
import pandas as pd
from pathlib import Path
from datetime import date

# ── Diretórios ─────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR  = BASE_DIR / "data" / "raw"  / "pncp"
OUT_PROC = BASE_DIR / "data" / "processed" / "pncp_licitacoes_pb.csv"

HOJE = date.today().strftime("%Y-%m-%d")

# Colunas finais a manter no processed — igual ao KEEP original do coletor
KEEP = [
    "numeroControlePNCP", "anoCompra", "processo",
    "modalidadeId", "modalidadeNome",
    "situacaoCompraId", "situacaoCompraNome",
    "dataPublicacaoPncp", "dataAberturaProposta", "dataEncerramentoProposta",
    "valorTotalEstimado", "valorTotalHomologado", "objetoCompra",
    "orgao_cnpj", "orgao_razaoSocial", "orgao_esfera",
    "municipio_ibge", "municipio_nome", "uf_unidade", "nomeUnidade",
    "_modalidade", "_modalidade_nome", "_mes",
]


def run() -> pd.DataFrame:
    """
    Consolida JSONL → DataFrame, faz flattening e seleciona colunas.
    Salva processed/pncp_licitacoes_pb.csv e snapshot datado em raw/.

    Retorna o DataFrame processado.
    """
    snap_jsonl = RAW_DIR / "pncp_parcial.jsonl"
    if not snap_jsonl.exists():
        raise FileNotFoundError(
            f"JSONL não encontrado: {snap_jsonl}\n"
            "Execute primeiro: python src/collectors/pncp.py"
        )

    # ── 1. Consolidação JSONL → DataFrame ─────────────────────────────────────
    print("[INFO] Lendo JSONL...")
    linhas = []
    with open(snap_jsonl, "r", encoding="utf-8") as fj:
        for linha in fj:
            try:
                obj = json.loads(linha)
                if not obj.get("_sem_dados"):
                    linhas.append(obj)
            except Exception:
                pass

    if not linhas:
        print("[WARNING] Nenhum dado disponível no JSONL.")
        return pd.DataFrame()

    df = pd.DataFrame(linhas)
    print(f"[INFO] {len(df):,} registros carregados do JSONL")

    # ── 2. Flattening de campos aninhados ─────────────────────────────────────
    if "orgaoEntidade" in df.columns:
        org = df["orgaoEntidade"].apply(lambda x: x if isinstance(x, dict) else {})
        df["orgao_cnpj"]        = org.apply(lambda x: x.get("cnpj", ""))
        df["orgao_razaoSocial"] = org.apply(lambda x: x.get("razaoSocial", ""))
        df["orgao_esfera"]      = org.apply(lambda x: x.get("esferaId", ""))
        df.drop(columns=["orgaoEntidade"], inplace=True)

    if "unidadeOrgao" in df.columns:
        uni = df["unidadeOrgao"].apply(lambda x: x if isinstance(x, dict) else {})
        df["municipio_ibge"] = uni.apply(lambda x: x.get("codigoIbge", ""))
        df["municipio_nome"] = uni.apply(lambda x: x.get("municipioNome", ""))
        df["uf_unidade"]     = uni.apply(lambda x: x.get("ufSigla", ""))
        df["nomeUnidade"]    = uni.apply(lambda x: x.get("nomeUnidade", ""))
        df.drop(columns=["unidadeOrgao"], inplace=True)

    # ── 3. Seleção de colunas ─────────────────────────────────────────────────
    df_out = df[[c for c in KEEP if c in df.columns]].copy()

    # ── 4. Exportação ─────────────────────────────────────────────────────────
    df_out.to_csv(OUT_PROC, index=False, encoding="utf-8-sig")

    snap_csv = RAW_DIR / f"pncp_snapshot_{HOJE}.csv"
    df_out.to_csv(snap_csv, index=False, encoding="utf-8-sig")

    muns = df_out["municipio_ibge"].replace("", pd.NA).nunique() if "municipio_ibge" in df_out.columns else "N/A"
    print(f"\n[SUCCESS] PNCP processado")
    print(f"   Registros         : {len(df_out):,}")
    print(f"   Municípios        : {muns}")
    print(f"   Modalidades       : {df_out['_modalidade'].nunique() if '_modalidade' in df_out.columns else 'N/A'}")
    print(f"   Arquivo principal : {OUT_PROC.name}")
    print(f"   Snapshot datado   : {snap_csv.name}")

    return df_out


if __name__ == "__main__":
    run()
