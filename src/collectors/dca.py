"""
Coletor DCA (Declaração de Contas Anuais) — SolveLicita
Responsabilidade: buscar Balanço Patrimonial (Anexo I-AB) e Balanço de
Receitas (Anexo I-C) para os municípios PB e salvar dados BRUTOS.

O cálculo de Scaixa e Autonomia é feito por:
    src/processors/dca_processor.py

2025 excluído do full: prazo de envio da DCA é abril/2026.
Atualizar ANOS_FULL para incluir 2025 a partir de maio/2026.

Rodar individualmente:
    python src/collectors/dca.py                     # full (2020–2024)
    python src/collectors/dca.py --mode incremental  # apenas último ano disponível
"""

import httpx
import pandas as pd
import time
import logging
import sys
from pathlib import Path

# ── Configuração ──────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent.parent
RAW_DCA   = BASE_DIR / "data" / "raw" / "dca"
PROCESSED = BASE_DIR / "data" / "processed"
RAW_DCA.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

API_BASE  = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/dca"
ANOS_FULL        = [2020, 2021, 2022, 2023, 2024]
ANOS_INCREMENTAL = [2024]   # último exercício consolidado disponível
ANEXO_BP  = "DCA-Anexo I-AB"
ANEXO_REC = "DCA-Anexo I-C"
DELAY     = 0.4
MAX_RETRY = 3

CONTA_ATIVO_FIN      = "Ativo Financeiro"
CONTA_PASSIVO_FIN    = "Passivo Financeiro"
CONTA_REC_TRIBUTARIA = "1.1.0.0.00.0.0 - Impostos, Taxas e Contribuições de Melhoria"
CONTA_REC_CORRENTE   = "RECEITAS (EXCETO INTRA-ORÇAMENTÁRIAS) (I)"
COLUNA_REALIZADO     = "Receitas Realizadas"


# ── Funções de busca e extração ───────────────────────────────────────────────

def fetch_dca(id_ente: str, ano: int, anexo: str,
              client: httpx.Client) -> list[dict]:
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
    """Revalida mapeamento de campos após atualizações da API. Uso pontual."""
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
    nome_lower = nome_conta.lower().strip()
    for item in items:
        if str(item.get("conta", "")).lower().strip() == nome_lower:
            try:
                return float(item.get("valor") or 0)
            except (ValueError, TypeError):
                return None
    return None


def extrair_receita(items: list[dict], nome_conta: str) -> float | None:
    nome_lower = nome_conta.lower().strip()

    for item in items:
        conta_ok  = str(item.get("conta",  "")).lower().strip() == nome_lower
        coluna_ok = str(item.get("coluna", "")).strip() == COLUNA_REALIZADO
        if conta_ok and coluna_ok:
            try:
                return float(item.get("valor") or 0)
            except (ValueError, TypeError):
                return None

    for item in items:
        if str(item.get("conta", "")).lower().strip() == nome_lower:
            try:
                return float(item.get("valor") or 0)
            except (ValueError, TypeError):
                return None

    return None


# ── Coleta principal ──────────────────────────────────────────────────────────

def coletar_dca(municipios: pd.DataFrame, anos: list[int],
                explorar: bool = False) -> pd.DataFrame:
    """
    Coleta DCA para os municípios e anos informados.
    Retorna DataFrame com valores brutos — sem cálculo de indicadores.
    """
    registros    = []
    n_registros  = len(municipios) * len(anos)
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

            for ano in anos:
                processados += 1
                log.info(f"[{processados:4d}/{n_registros}] {nome} ({cod}) — {ano}")

                items_bp = fetch_dca(cod, ano, ANEXO_BP, client)
                time.sleep(DELAY)

                ativo_fin = passivo_fin = None
                if items_bp:
                    ativo_fin   = extrair_bp(items_bp, CONTA_ATIVO_FIN)
                    passivo_fin = extrair_bp(items_bp, CONTA_PASSIVO_FIN)

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


def _salvar_com_merge(df_novo: pd.DataFrame, caminho: Path) -> pd.DataFrame:
    """
    Salva df_novo. Se o arquivo já existir (modo incremental),
    concatena e desuplica por (cod_ibge, ano) — mantendo o mais recente.
    """
    if caminho.exists():
        df_existente = pd.read_csv(caminho, dtype={"cod_ibge": str})
        df_final = pd.concat([df_existente, df_novo], ignore_index=True)
        df_final = df_final.drop_duplicates(subset=["cod_ibge", "ano"], keep="last")
    else:
        df_final = df_novo

    df_final.to_csv(caminho, index=False)
    return df_final


def run(mode: str = "full", municipios: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Executa a coleta DCA e salva raw/dca/dca_raw_pb.csv.

    Parâmetros
    ----------
    mode       : "full"        — coleta ANOS_FULL completo
                 "incremental" — coleta ANOS_INCREMENTAL e faz merge no raw existente
    municipios : DataFrame de municípios PB. Se None, lê de processed/municipios_pb_tabela.csv.

    Retorna o DataFrame bruto final (histórico completo após merge).
    """
    if municipios is None:
        path_mun = PROCESSED / "municipios_pb_tabela.csv"
        if not path_mun.exists():
            raise FileNotFoundError(
                f"Tabela de municípios não encontrada: {path_mun}\n"
                "Execute primeiro: python src/collectors/municipios.py"
            )
        municipios = pd.read_csv(path_mun, dtype={"cod_ibge": str})
        log.info(f"  {len(municipios)} municípios carregados")

    anos = ANOS_FULL if mode == "full" else ANOS_INCREMENTAL
    log.info(f"  Modo DCA: {mode.upper()} | Anos: {anos}")

    n_reg = len(municipios) * len(anos)
    log.info(
        f"\nIniciando coleta DCA "
        f"({n_reg} registros | {n_reg * 2} requisições | "
        f"~{n_reg * 2 * DELAY / 60:.0f} min)..."
    )

    # explorar_campos: descomentar para revalidar campos após atualização da API
    # coletar_dca(municipios, anos, explorar=True)

    df_novo    = coletar_dca(municipios, anos)
    caminho    = RAW_DCA / "dca_raw_pb.csv"
    df_final   = _salvar_com_merge(df_novo, caminho)

    log.info(f"  ✅ Raw salvo: {caminho} ({len(df_final)} linhas, {df_final['ano'].nunique()} anos)")
    return df_final


if __name__ == "__main__":
    mode = "incremental" if "--mode" in sys.argv and "incremental" in sys.argv else "full"
    run(mode=mode)
