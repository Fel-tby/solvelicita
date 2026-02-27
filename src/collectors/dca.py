"""
Coletor DCA (Declaração de Contas Anuais) — SolveLicita
Fase 0 — coleta Balanço Patrimonial (Anexo I-AB) e Balanço de Receitas (Anexo I-C)
para os 223 municípios da Paraíba, anos 2020–2024.

Variáveis extraídas:
  - Scaixa   : (Ativo Financeiro - Passivo Financeiro) / Receita Corrente
  - Autonomia: Receita Tributária / Receita Corrente Total (exceto intra-orçamentárias)

Campos confirmados via exploração em 27/02/2026 (Campina Grande, 2024):
  Anexo I-AB : col_conta='conta', col_valor='valor'
               contas consolidadas: 'Ativo Financeiro', 'Passivo Financeiro'
  Anexo I-C  : col_conta='conta', col_valor='valor', col_tipo='coluna'
               receita tributária: '1.1.0.0.00.0.0 - Impostos, Taxas e Contribuições de Melhoria'
               receita corrente  : 'RECEITAS (EXCETO INTRA-ORÇAMENTÁRIAS) (I)'
               filtro ideal       : coluna == 'Receitas Realizadas' (com fallback sem filtro)

2025 excluído: prazo de envio da DCA é abril/2026 — exercício ainda não consolidado.
Atualizar ANOS para [2021, 2022, 2023, 2024, 2025] a partir de maio/2026.
"""

import httpx
import pandas as pd
import time
import logging
from pathlib import Path

# ── Configuração ──────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent.parent
PROCESSED = BASE_DIR / "data" / "processed"
RAW_DCA   = BASE_DIR / "data" / "raw" / "dca"
RAW_DCA.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

API_BASE  = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/dca"
ANOS      = [2020, 2021, 2022, 2023, 2024]
ANEXO_BP  = "DCA-Anexo I-AB"
ANEXO_REC = "DCA-Anexo I-C"
DELAY     = 0.4
MAX_RETRY = 3

# ── Mapeamento de contas (confirmado via exploração 27/02/2026) ───────────────
CONTA_ATIVO_FIN      = "Ativo Financeiro"
CONTA_PASSIVO_FIN    = "Passivo Financeiro"
CONTA_REC_TRIBUTARIA = "1.1.0.0.00.0.0 - Impostos, Taxas e Contribuições de Melhoria"
CONTA_REC_CORRENTE   = "RECEITAS (EXCETO INTRA-ORÇAMENTÁRIAS) (I)"
COLUNA_REALIZADO     = "Receitas Realizadas"


# ── Funções auxiliares ────────────────────────────────────────────────────────

def fetch_dca(id_ente: str, ano: int, anexo: str,
              client: httpx.Client) -> list[dict]:
    """Busca um anexo DCA. Retorna lista de itens ou [] em caso de falha/404."""
    params = {"an_exercicio": ano, "no_anexo": anexo, "id_ente": id_ente}
    for tentativa in range(1, MAX_RETRY + 1):
        try:
            r = client.get(API_BASE, params=params, timeout=30)
            r.raise_for_status()
            return r.json().get("items", [])
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []
            log.warning(f"  HTTP {e.response.status_code} | {id_ente} {ano} | tentativa {tentativa}")
        except Exception as e:
            log.warning(f"  Erro: {e} | {id_ente} {ano} | tentativa {tentativa}")
        time.sleep(DELAY * tentativa)
    return []


def explorar_campos(id_ente: str, ano: int, anexo: str,
                    client: httpx.Client) -> None:
    """
    Imprime campos e contas únicas retornados pela API.
    Usar apenas para revalidar mapeamento após mudanças na API.
    """
    items = fetch_dca(id_ente, ano, anexo, client)
    if not items:
        log.warning(f"Nenhum dado: {id_ente} {ano} {anexo}")
        return
    log.info(f"\n{'='*60}")
    log.info(f"EXPLORAÇÃO: {anexo} | ente {id_ente} | ano {ano}")
    log.info(f"Campos: {list(items[0].keys())}")
    log.info(f"Valores únicos de 'coluna': {sorted({i.get('coluna','') for i in items})}")
    log.info("Contas únicas:")
    for c in sorted({i.get("conta", "") for i in items}):
        log.info(f"  {c}")
    log.info(f"{'='*60}\n")


def extrair_bp(items: list[dict], nome_conta: str) -> float | None:
    """
    Extrai valor do Balanço Patrimonial pelo nome exato da conta.
    O BP não tem coluna de tipo — retorna o primeiro match direto.
    """
    nome_lower = nome_conta.lower().strip()
    for item in items:
        if str(item.get("conta", "")).lower().strip() == nome_lower:
            try:
                return float(item.get("valor") or 0)
            except (ValueError, TypeError):
                return None
    return None


def extrair_receita(items: list[dict], nome_conta: str) -> float | None:
    """
    Extrai valor do Balanço de Receitas pelo nome exato da conta.

    Tenta primeiro com filtro coluna == 'Receitas Realizadas' (ideal).
    Se não encontrar — inconsistência de nomenclatura entre anos ou municípios —
    cai no fallback sem filtro, pegando o primeiro match pelo nome da conta.

    O fallback é necessário porque contas intermediárias (ex: 1.1.0.0.00.0.0)
    em alguns exercícios não têm o campo 'coluna' preenchido com o valor padrão.
    """
    nome_lower = nome_conta.lower().strip()

    # Tentativa 1: match exato conta + coluna 'Receitas Realizadas'
    for item in items:
        conta_ok  = str(item.get("conta",  "")).lower().strip() == nome_lower
        coluna_ok = str(item.get("coluna", "")).strip() == COLUNA_REALIZADO
        if conta_ok and coluna_ok:
            try:
                return float(item.get("valor") or 0)
            except (ValueError, TypeError):
                return None

    # Fallback: match só pelo nome da conta (sem filtro de coluna)
    for item in items:
        if str(item.get("conta", "")).lower().strip() == nome_lower:
            try:
                return float(item.get("valor") or 0)
            except (ValueError, TypeError):
                return None

    return None


# ── Coleta principal ──────────────────────────────────────────────────────────

def coletar_dca(municipios: pd.DataFrame,
                explorar: bool = False) -> pd.DataFrame:
    """
    Coleta DCA para todos os municípios e anos definidos.

    Parâmetros
    ----------
    municipios : DataFrame com colunas cod_ibge, ente, populacao
    explorar   : se True, roda apenas exploração de campos e encerra.
                 Usar para revalidar mapeamento após atualizações da API.
    """
    registros = []
    n_registros  = len(municipios) * len(ANOS)
    n_requisicoes = n_registros * 2
    processados  = 0

    with httpx.Client(follow_redirects=True) as client:

        if explorar:
            explorar_campos("2504009", 2024, ANEXO_BP,  client)
            explorar_campos("2504009", 2024, ANEXO_REC, client)
            return pd.DataFrame()

        for _, mun in municipios.iterrows():
            cod  = str(mun["cod_ibge"])
            nome = mun["ente"]
            pop  = mun.get("populacao", 0)

            for ano in ANOS:
                processados += 1
                log.info(f"[{processados:4d}/{n_registros}] {nome} ({cod}) — {ano}")

                # ── Balanço Patrimonial (Anexo I-AB) ──────────────────────────
                items_bp = fetch_dca(cod, ano, ANEXO_BP, client)
                time.sleep(DELAY)

                ativo_fin = passivo_fin = None
                if items_bp:
                    ativo_fin   = extrair_bp(items_bp, CONTA_ATIVO_FIN)
                    passivo_fin = extrair_bp(items_bp, CONTA_PASSIVO_FIN)

                # ── Balanço de Receitas (Anexo I-C) ───────────────────────────
                items_rec = fetch_dca(cod, ano, ANEXO_REC, client)
                time.sleep(DELAY)

                rec_trib = rec_corr = None
                if items_rec:
                    rec_trib = extrair_receita(items_rec, CONTA_REC_TRIBUTARIA)
                    rec_corr = extrair_receita(items_rec, CONTA_REC_CORRENTE)

                registros.append({
                    "cod_ibge":           cod,
                    "ente":               nome,
                    "populacao":          pop,
                    "ano":                ano,
                    "ativo_financeiro":   ativo_fin,
                    "passivo_financeiro": passivo_fin,
                    "rec_tributaria":     rec_trib,
                    "rec_corrente":       rec_corr,
                    "bp_disponivel":      bool(items_bp),
                    "rec_disponivel":     bool(items_rec),
                })

    return pd.DataFrame(registros)


# ── Processamento pós-coleta ──────────────────────────────────────────────────

def calcular_indicadores(df: pd.DataFrame,
                          df_rreo: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calcula Scaixa e Autonomia a partir dos dados brutos.

    Scaixa    = (Ativo Financeiro - Passivo Financeiro) / Receita Corrente
    Autonomia = Receita Tributária / Receita Corrente

    Denominador: rec_corrente do Anexo I-C.
    Fallback: média da receita realizada do RREO (siconfi_indicadores_pb.csv).
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

    # fillna explícito — evita FutureWarning do combine_first com dtype object
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

    # Garante dtype float antes do groupby — evita TypeError no nlargest
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

    log.info("\n" + "="*60)
    log.info("DIAGNÓSTICO — Indicadores DCA")
    log.info("="*60)
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

    # nlargest/nsmallest só se Scaixa tiver dados e dtype correto
    if not sc.empty:
        cols = ["ente", "populacao", "scaixa_medio", "autonomia_media"]
        log.info("\nTop 5 — melhor Scaixa:")
        log.info(df.nlargest(5, "scaixa_medio")[cols].to_string(index=False))
        log.info("\nBottom 5 — pior Scaixa:")
        log.info(df.nsmallest(5, "scaixa_medio")[cols].to_string(index=False))


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Carregando tabela de municípios...")
    municipios = pd.read_csv(PROCESSED / "municipios_pb_tabela.csv",
                             dtype={"cod_ibge": str})
    log.info(f"  {len(municipios)} municípios carregados")

    n_reg = len(municipios) * len(ANOS)
    log.info(f"\nIniciando coleta DCA "
             f"({n_reg} registros | {n_reg * 2} requisições | "
             f"~{n_reg * 2 * DELAY / 60:.0f} min)...")

    # ── Exploração: descomentar para revalidar campos após atualização da API ──
    # coletar_dca(municipios, explorar=True)

    # ── Coleta em massa ───────────────────────────────────────────────────────
    df_raw = coletar_dca(municipios)
    df_raw.to_csv(RAW_DCA / "dca_raw_pb.csv", index=False)
    log.info(f"  Raw salvo: {RAW_DCA / 'dca_raw_pb.csv'}")

    # ── Calcular indicadores ──────────────────────────────────────────────────
    log.info("\nCalculando indicadores...")
    df_rreo = pd.read_csv(PROCESSED / "siconfi_indicadores_pb.csv",
                          dtype={"cod_ibge": str})
    df_det, df_media = calcular_indicadores(df_raw, df_rreo)

    df_det.to_csv(PROCESSED   / "dca_indicadores_pb_detalhado.csv", index=False)
    df_media.to_csv(PROCESSED / "dca_indicadores_pb.csv",           index=False)
    log.info(f"  Processado: {PROCESSED / 'dca_indicadores_pb.csv'}")

    # ── Diagnóstico ───────────────────────────────────────────────────────────
    diagnostico(df_media, municipios)
    log.info("\n✅ Coleta DCA concluída.")