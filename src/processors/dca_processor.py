"""
Processador DCA — calcula Scaixa e Autonomia para municípios PB.
Lê o CSV bruto produzido por src/collectors/dca.py e deriva:
    scaixa_raw    = (Ativo Financeiro - Passivo Financeiro) / Receita Corrente
    autonomia_raw = Receita Tributária / Receita Corrente

Input:
    raw/dca/dca_raw_pb.csv                    (produzido por collectors/dca.py)
    processed/siconfi_indicadores_pb.csv      (fallback de receita corrente via RREO)

Output:
    processed/dca_indicadores_pb_detalhado.csv  — série histórica por município/ano
    processed/dca_indicadores_pb.csv            — médias agregadas por município

Rodar individualmente:
    python src/processors/dca_processor.py
"""

import pandas as pd
import logging
from pathlib import Path

# ── Configuração ──────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent.parent
RAW_DCA   = BASE_DIR / "data" / "raw" / "dca"
PROCESSED = BASE_DIR / "data" / "processed"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Processamento ─────────────────────────────────────────────────────────────

def calcular_indicadores(df: pd.DataFrame,
                          df_rreo: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calcula Scaixa e Autonomia a partir dos dados brutos.

    Scaixa    = (Ativo Financeiro - Passivo Financeiro) / Receita Corrente
    Autonomia = Receita Tributária / Receita Corrente

    Denominador: rec_corrente do Anexo I-C.
    Fallback: média da receita realizada do RREO (siconfi_indicadores_pb.csv).

    Retorna (df_detalhado, df_media):
        df_detalhado — série histórica com scaixa_raw e autonomia_raw por ano
        df_media     — médias agregadas por município (entrada para solvency.py)
    """
    df = df.copy()

    # ── Fallback de receita corrente via RREO ─────────────────────────────────
    for col_candidata in ["receita_realizada", "rcl", "receita_corrente_liquida"]:
        if col_candidata in df_rreo.columns:
            rcl_rreo = (
                df_rreo.groupby("cod_ibge")[col_candidata]
                .mean()
                .reset_index()
                .rename(columns={col_candidata: "rcl_rreo"})
            )
            df = df.merge(rcl_rreo, on="cod_ibge", how="left")
            break
    else:
        df["rcl_rreo"] = float("nan")

    df["rcl_rreo"]           = pd.to_numeric(df["rcl_rreo"],     errors="coerce")
    df["rec_corrente"]       = pd.to_numeric(df["rec_corrente"], errors="coerce")
    df["rec_corrente_final"] = df["rec_corrente"].fillna(df["rcl_rreo"])

    # ── Scaixa ────────────────────────────────────────────────────────────────
    df["scaixa_raw"] = float("nan")
    mask_sc = (
        df["ativo_financeiro"].notna()   &
        df["passivo_financeiro"].notna() &
        df["rec_corrente_final"].notna() &
        (df["rec_corrente_final"] > 0)
    )
    df.loc[mask_sc, "scaixa_raw"] = (
        (df.loc[mask_sc, "ativo_financeiro"] - df.loc[mask_sc, "passivo_financeiro"]) /
        df.loc[mask_sc, "rec_corrente_final"]
    )

    # ── Autonomia ─────────────────────────────────────────────────────────────
    df["autonomia_raw"] = float("nan")
    mask_au = (
        df["rec_tributaria"].notna()      &
        df["rec_corrente_final"].notna()  &
        (df["rec_corrente_final"] > 0)
    )
    df.loc[mask_au, "autonomia_raw"] = (
        df.loc[mask_au, "rec_tributaria"] /
        df.loc[mask_au, "rec_corrente_final"]
    )

    df["scaixa_raw"]    = pd.to_numeric(df["scaixa_raw"],    errors="coerce")
    df["autonomia_raw"] = pd.to_numeric(df["autonomia_raw"], errors="coerce")

    # ── Média 2020–2024 por município ─────────────────────────────────────────
    media = (
        df.groupby("cod_ibge")
        .agg(
            scaixa_medio    = ("scaixa_raw",    "mean"),
            autonomia_media = ("autonomia_raw",  "mean"),
            anos_bp_ok      = ("bp_disponivel",  "sum"),
            anos_rec_ok     = ("rec_disponivel", "sum"),
        )
        .reset_index()
    )
    return df, media


# ── Diagnóstico ───────────────────────────────────────────────────────────────

def diagnostico(media: pd.DataFrame, municipios: pd.DataFrame) -> None:
    df = municipios.merge(media, on="cod_ibge", how="left")

    log.info("\n" + "=" * 60)
    log.info("DIAGNÓSTICO — Indicadores DCA")
    log.info("=" * 60)
    log.info(f"  Scaixa    coletado : {media['scaixa_medio'].notna().sum()}/{len(media)} municípios")
    log.info(f"  Autonomia coletada : {media['autonomia_media'].notna().sum()}/{len(media)} municípios")

    sc = media["scaixa_medio"].dropna()
    if not sc.empty:
        log.info(f"\nScaixa  — média: {sc.mean():.4f} | mediana: {sc.median():.4f} "
                 f"| min: {sc.min():.4f} | max: {sc.max():.4f}")
        log.info(f"  Insolventes (< 0)   : {(sc < 0).sum()} municípios")
        log.info(f"  Folga (> 0.10)      : {(sc > 0.10).sum()} municípios")

    au = media["autonomia_media"].dropna()
    if not au.empty:
        log.info(f"\nAutonomia — média: {au.mean():.4f} | mediana: {au.median():.4f} "
                 f"| min: {au.min():.4f} | max: {au.max():.4f}")
        log.info(f"  Crítico (< 5%)      : {(au < 0.05).sum()} municípios")
        log.info(f"  Bom    (> 20%)      : {(au > 0.20).sum()} municípios")

    if not sc.empty:
        cols = ["ente", "populacao", "scaixa_medio", "autonomia_media"]
        log.info("\nTop 5 — melhor Scaixa:")
        log.info(df.nlargest(5, "scaixa_medio")[cols].to_string(index=False))
        log.info("\nBottom 5 — pior Scaixa:")
        log.info(df.nsmallest(5, "scaixa_medio")[cols].to_string(index=False))


# ── Entry point ───────────────────────────────────────────────────────────────

def run(df_raw: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Calcula indicadores DCA e salva CSVs processados.

    Parâmetros
    ----------
    df_raw : DataFrame bruto do coletor. Se None, lê de raw/dca/dca_raw_pb.csv.

    Retorna df_media (médias por município — entrada para solvency.py).
    """
    # ── Carga do raw ──────────────────────────────────────────────────────────
    if df_raw is None:
        path_raw = RAW_DCA / "dca_raw_pb.csv"
        if not path_raw.exists():
            raise FileNotFoundError(
                f"Raw DCA não encontrado: {path_raw}\n"
                "Execute primeiro: python src/collectors/dca.py"
            )
        df_raw = pd.read_csv(path_raw, dtype={"cod_ibge": str})
        log.info(f"  Raw DCA: {len(df_raw)} linhas")

    # ── Fallback RREO (receita corrente) ──────────────────────────────────────
    path_siconfi = PROCESSED / "siconfi_indicadores_pb.csv"
    if not path_siconfi.exists():
        raise FileNotFoundError(
            f"SICONFI indicadores não encontrado: {path_siconfi}\n"
            "Execute primeiro: python src/processors/siconfi_processor.py"
        )
    df_rreo = pd.read_csv(path_siconfi, dtype={"cod_ibge": str})

    # ── Cálculo ───────────────────────────────────────────────────────────────
    log.info("\nCalculando indicadores...")
    df_det, df_media = calcular_indicadores(df_raw, df_rreo)

    # ── Exportação ────────────────────────────────────────────────────────────
    df_det.to_csv(PROCESSED   / "dca_indicadores_pb_detalhado.csv", index=False)
    df_media.to_csv(PROCESSED / "dca_indicadores_pb.csv",           index=False)
    log.info(f"  ✅ Processado: {PROCESSED / 'dca_indicadores_pb.csv'}")

    # ── Diagnóstico ───────────────────────────────────────────────────────────
    path_mun = PROCESSED / "municipios_pb_tabela.csv"
    if path_mun.exists():
        municipios = pd.read_csv(path_mun, dtype={"cod_ibge": str})
        diagnostico(df_media, municipios)

    log.info("\n✅ DCA processado.")
    return df_media


if __name__ == "__main__":
    run()
