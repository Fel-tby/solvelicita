"""
Processador CAUC — classifica pendências fiscais dos municípios PB.
Lê o CSV bruto produzido por src/collectors/cauc.py e deriva:
    bloqueado, qtd_pendencias, pendencias (lista legível por gravidade)

Input:  raw/cauc/cauc_raw_pb.csv
Output: processed/cauc_situacao_pb.csv

Rodar individualmente:
    python src/processors/cauc_processor.py
"""

import pandas as pd
from pathlib import Path
from datetime import date

# ── Diretórios ─────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR  = BASE_DIR / "data" / "raw" / "cauc"
OUT_PROC = BASE_DIR / "data" / "processed" / "cauc_situacao_pb.csv"

HOJE = date.today().strftime("%Y-%m-%d")

# ── Dicionário de requisitos de regularidade ──────────────────────────────────
# Chaves = códigos das colunas no CSV do CKAN.
# Valores = descrição legível para o campo "pendencias" do output.
REQUISITOS = {
    "1.1":   "Regularidade Previdenciária (RPPS)",
    "1.2":   "Regularidade Fiscal (RFB)",
    "1.3":   "Regularidade PGFN",
    "1.4":   "Regularidade FGTS",
    "1.5":   "Regularidade Trabalhista (TST)",
    "2.1.1": "LRF - Limite Pessoal Executivo",
    "2.1.2": "LRF - Limite Pessoal Legislativo",
    "3.1.1": "SIOPS (Saúde)",
    "3.1.2": "SIOPS Demonstrativo",
    "3.2.1": "SIOPE (Educação)",
    "3.2.2": "SIOPE Demonstrativo",
    "3.2.3": "SIOPE Complementar",
    "3.2.4": "SIOPE Observações",
    "3.3":   "SIGA (Alimentação Escolar)",
    "3.4.1": "SICONV/TRANSFEREGOV Prestação de Contas",
    "3.4.2": "SICONV/TRANSFEREGOV Débitos",
    "3.5":   "CADIN",
    "3.6":   "Adimplência TCU",
    "3.7":   "Adimplência CGU",
    "4.1":   "SISTN (Dívida Consolidada)",
    "4.2":   "SISTN (Garantias)",
    "5.1":   "SICONFI RREO",
    "5.2":   "SICONFI RGF",
    "5.3":   "SICONFI Balanço Anual",
    "5.4":   "SICONFI DCA",
    "5.5":   "SICONFI PCASP",
    "5.6":   "SICONFI DCASP",
    "5.7":   "SICONFI MCASP",
}


def run(df_raw: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Classifica pendências CAUC e salva processed/cauc_situacao_pb.csv.

    Parâmetros
    ----------
    df_raw : DataFrame bruto do coletor (todas as colunas originais do CKAN).
             Se None, lê de raw/cauc/cauc_raw_pb.csv.

    Retorna o DataFrame processado com os campos derivados.
    """
    # ── Carga ─────────────────────────────────────────────────────────────────
    if df_raw is None:
        path_raw = RAW_DIR / "cauc_raw_pb.csv"
        if not path_raw.exists():
            raise FileNotFoundError(
                f"Raw CAUC não encontrado: {path_raw}\n"
                "Execute primeiro: python src/collectors/cauc.py"
            )
        df_raw = pd.read_csv(path_raw, dtype=str, na_filter=False)
        print(f"  Lido: {path_raw.name} ({len(df_raw)} linhas)")

    # ── Detecta colunas dinâmicas ─────────────────────────────────────────────
    col_ibge = next((c for c in df_raw.columns if "ibge" in c.lower()), None)
    if not col_ibge:
        raise ValueError(f"Coluna IBGE não encontrada. Colunas: {list(df_raw.columns)}")

    col_nome    = next((c for c in df_raw.columns
                        if "nome" in c.lower() and "ente" in c.lower()), None)
    colunas_req = [c for c in df_raw.columns if c in REQUISITOS]

    # ── Classificação linha a linha ───────────────────────────────────────────
    registros = []
    for _, row in df_raw.iterrows():
        cod      = row[col_ibge]
        nome_row = row.get(col_nome, "") if col_nome else ""
        data_p   = row.get("data_pesquisa", "")
        data_c   = row.get("data_coleta", HOJE)

        # Pendência identificada por "!" ou campo vazio na coluna do requisito
        pendencias = [
            REQUISITOS[c] for c in colunas_req
            if row.get(c, "").strip() in ("!", "")
        ]
        bloqueado = len(pendencias) > 0

        registros.append({
            "cod_ibge":       cod,
            "municipio":      nome_row,
            "bloqueado":      bloqueado,
            "qtd_pendencias": len(pendencias),
            "pendencias":     " | ".join(pendencias) if pendencias else "REGULAR",
            "data_pesquisa":  data_p,
            "data_coleta":    data_c,
            "fonte":          "CKAN-TesouroTransparente",
        })

    df_final = pd.DataFrame(registros)

    # ── Exportação ────────────────────────────────────────────────────────────
    df_final.to_csv(OUT_PROC, index=False, encoding="utf-8-sig")

    bloqueados = df_final["bloqueado"].sum()
    print(f"\n✅ CAUC processado:")
    print(f"   Total PB       : {len(df_final)}")
    print(f"   Com pendências : {bloqueados}")
    print(f"   Regulares      : {len(df_final) - bloqueados}")
    print(f"   Salvo em       : {OUT_PROC.name}")

    return df_final


if __name__ == "__main__":
    run()
