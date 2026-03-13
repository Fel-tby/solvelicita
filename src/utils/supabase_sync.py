"""
src/utils/supabase_sync.py
"""

import os
import json
import math
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

CSV_PATH = BASE_DIR / "data" / "outputs" / "score_municipios_pb_pncp.csv"

COLUNAS = {
    "cod_ibge":               "cod_ibge",
    "ente":                   "ente",
    "populacao":              "populacao",
    "score":                  "score",
    "classificacao":          "classificacao",
    "contrib_lliq":           "contrib_lliq",
    "contrib_ccauc":          "contrib_ccauc",
    "contrib_eorcam":         "contrib_eorcam",
    "contrib_qsiconfi":       "contrib_qsiconfi",
    "contrib_autonomia":      "contrib_autonomia",
    "contrib_rproc":          "contrib_rproc",
    "lliq_raw":               "lliq_raw",
    "eorcam_raw":             "eorcam_raw",
    "rrestos_nproc_pct":      "rrestos_nproc_pct",
    "qsiconfi":               "qsiconfi",
    "ccauc":                  "ccauc",
    "autonomia_media":        "autonomia_media",
    "lliq_norm":              "lliq_norm",
    "eorcam_norm":            "eorcam_norm",
    "rproc_norm":             "rproc_norm",
    "autonomia_norm":         "autonomia_norm",
    "score_base":             "score_base",
    "score_bruto":            "score_bruto",
    "pen_lliq_parcial":       "pen_lliq_parcial",
    "pen_situacional":        "pen_situacional",
    "n_anos_cronicos":        "n_anos_cronicos",
    "anos_entregues":         "anos_entregues",
    "lliq_parcial":           "lliq_parcial",
    "dado_defasado":          "dado_defasado",
    "dado_suspeito":          "dado_suspeito",
    "dado_suspeito_lliq":     "dado_suspeito_lliq",
    "autonomia_critica":      "autonomia_critica",
    "dias_atraso":            "dias_atraso",
    "decay_fator":            "decay_fator",
    "n_licitacoes":           "n_licitacoes",
    "valor_homologado_total": "valor_homologado_total",
    "n_dispensa":             "n_dispensa",
    "valor_hom_dispensa":     "valor_hom_dispensa",
    "ano_ultima_licitacao":   "ano_ultima_licitacao",
    "pct_dispensa":           "pct_dispensa",
    "alerta_dispensa":        "alerta_dispensa",
}

# Colunas que o PostgreSQL espera como integer mas pandas lê como float
# (ocorre quando a coluna tem NaN — pandas promove int → float automaticamente)
COLUNAS_INTEGER = {
    "n_licitacoes", "n_dispensa", "ano_ultima_licitacao",
    "n_anos_cronicos", "anos_entregues", "populacao",
}

COLUNAS_BOOLEAN = {
    "lliq_parcial", "dado_defasado", "dado_suspeito",
    "dado_suspeito_lliq", "autonomia_critica", "alerta_dispensa",
}


def _conectar() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise EnvironmentError(
            "SUPABASE_URL e SUPABASE_KEY precisam estar definidos no .env"
        )
    return create_client(url, key)


def _sanitizar(rec: dict) -> dict:
    NAN_STRINGS = {"NaN", "nan", "None", "none", "inf", "-inf", "Inf", "-Inf"}
    resultado = {}
    for k, v in rec.items():
        if v is None:
            resultado[k] = None
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            resultado[k] = None
        elif isinstance(v, str) and v in NAN_STRINGS:
            resultado[k] = None
        elif k in COLUNAS_INTEGER and isinstance(v, float):
            resultado[k] = int(v)
        else:
            resultado[k] = v
    return resultado


def _preparar_registros(caminho: Path) -> list:
    df = pd.read_csv(caminho, dtype={"cod_ibge": str})

    colunas_presentes = {k: v for k, v in COLUNAS.items() if k in df.columns}
    ausentes = set(COLUNAS.keys()) - set(colunas_presentes.keys())
    if ausentes:
        print(f"  ⚠️  Colunas ausentes no CSV (ignoradas): {sorted(ausentes)}")

    df = df[list(colunas_presentes.keys())].rename(columns=colunas_presentes)

    # Booleanos
    for col in COLUNAS_BOOLEAN:
        if col in df.columns:
            df[col] = df[col].map(lambda v: bool(v) if pd.notnull(v) else None)

    # to_json converte NaN → null nativamente
    registros = json.loads(
        df.to_json(orient="records", force_ascii=False, date_format="iso")
    )

    # Sanitiza NaN remanescentes e converte floats inteiros
    return [_sanitizar(r) for r in registros]


def run() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(
            f"CSV não encontrado: {CSV_PATH}\n"
            "Execute primeiro a etapa 'score' do pipeline."
        )

    print("Conectando ao Supabase...")
    supabase = _conectar()

    print(f"Lendo {CSV_PATH.name}...")
    registros = _preparar_registros(CSV_PATH)

    print(f"Enviando {len(registros)} municípios para o Supabase...")

    LOTE = 100
    total_atualizados = 0
    for i in range(0, len(registros), LOTE):
        lote = registros[i: i + LOTE]
        response = (
            supabase.table("municipios")
            .upsert(lote, on_conflict="cod_ibge")
            .execute()
        )
        n = len(response.data) if response.data else 0
        total_atualizados += n
        print(f"  Lote {i // LOTE + 1}: {n} registros")

    print(f"✅ Supabase sincronizado — {total_atualizados} registros atualizados.")


if __name__ == "__main__":
    run()