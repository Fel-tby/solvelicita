# src/collectors/pncp.py
import requests
import pandas as pd
import time
import json
from pathlib import Path
from datetime import date
from calendar import monthrange

# Configurações de Diretórios
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR  = BASE_DIR / "data" / "raw" / "pncp"
OUT_PROC = BASE_DIR / "data" / "processed" / "pncp_licitacoes_pb.csv"

# Garante a existência da estrutura de pastas
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Parâmetros da API
BASE_URL    = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
DATA_INICIO = date(2023, 1, 1)
DATA_FIM    = date.today()
HOJE        = date.today().strftime("%Y-%m-%d")

MODALIDADES = {
    1:  "Leilão Eletrônico",
    2:  "Diálogo Competitivo",
    3:  "Concurso",
    4:  "Concorrência Eletrônica",
    5:  "Concorrência Presencial",
    6:  "Pregão Eletrônico",
    7:  "Pregão Presencial",
    8:  "Dispensa de Licitação",
    9:  "Inexigibilidade",
    10: "Manifestação de Interesse",
    11: "Pré-qualificação",
    12: "Credenciamento",
    13: "Leilão Presencial",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept":     "application/json",
}

# Configurações de Rate Limit e Retries
TAMANHO_PAGINA   = 50
SLEEP_PAGINA     = 0.5
SLEEP_MES        = 0.4
SLEEP_MODALIDADE = 2.0
BACKOFF_429      = 60
MAX_RETRIES      = 6


def gerar_meses(inicio: date, fim: date) -> list[tuple[date, date]]:
    """
    Gera uma lista de tuplas contendo o primeiro e o último dia de cada mês
    dentro do intervalo especificado.
    """
    meses = []
    ano, mes = inicio.year, inicio.month
    while date(ano, mes, 1) <= fim:
        ultimo_dia = monthrange(ano, mes)[1]
        fim_mes = min(date(ano, mes, ultimo_dia), fim)
        meses.append((date(ano, mes, 1), fim_mes))
        mes += 1
        if mes > 12:
            mes = 1
            ano += 1
    return meses


def fetch_com_backoff(params: dict) -> dict | None:
    """
    Executa requisição GET na API do PNCP implementando backoff exponencial.
    
    Returns:
        dict: Payload JSON da resposta.
        dict: Com chave 'empty=True' em caso de status 204 (No Content).
        None: Em caso de falha definitiva após esgotamento de retries.
    """
    for t in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 204:
                return {"data": [], "totalRegistros": 0, "totalPaginas": 0, "empty": True}

            if r.status_code == 429:
                espera = BACKOFF_429 * t
                print(f"\n    [WARNING] 429 Rate Limit. Aguardando {espera}s (Tentativa {t}/{MAX_RETRIES})")
                time.sleep(espera)
                continue

            if r.status_code in (500, 502, 503, 504):
                espera = 15 * t
                print(f"\n    [WARNING] {r.status_code} Server Error. Aguardando {espera}s")
                time.sleep(espera)
                continue

            print(f"\n    [ERROR] HTTP {r.status_code}: {r.text[:120]}")
            return None

        except requests.exceptions.Timeout:
            print(f"\n    [WARNING] Timeout. (Tentativa {t})")
            time.sleep(10 * t)
        except requests.exceptions.ConnectionError:
            print(f"\n    [WARNING] Connection Error. (Tentativa {t})")
            time.sleep(20 * t)

    print(f"\n    [ERROR] Falha definitiva após {MAX_RETRIES} tentativas.")
    return None


def coletar_bloco(modalidade: int, data_ini: date, data_fim: date) -> tuple[list, bool]:
    """
    Coleta registros paginados de uma modalidade específica em um intervalo de datas.

    Returns:
        tuple[list, bool]: Lista de registros coletados e flag booleana indicando
                           sucesso na operação da API (útil para controle de checkpoint).
    """
    params_base = {
        "dataInicial":                 data_ini.strftime("%Y%m%d"),
        "dataFinal":                   data_fim.strftime("%Y%m%d"),
        "codigoModalidadeContratacao": modalidade,
        "uf":                          "PB",
        "tamanhoPagina":               TAMANHO_PAGINA,
        "pagina":                      1,
    }

    resp = fetch_com_backoff(params_base)

    if resp is None:
        return [], False

    if resp.get("empty") or not resp.get("data"):
        return [], True

    registros   = list(resp["data"])
    total_pags  = resp.get("totalPaginas", 1)
    total_regs  = resp.get("totalRegistros", 0)

    if total_regs > 0:
        print(f" {total_regs} regs / {total_pags} págs", end="", flush=True)

    # Iteração sobre as páginas subsequentes
    for pag in range(2, total_pags + 1):
        time.sleep(SLEEP_PAGINA)
        resp_pag = fetch_com_backoff({**params_base, "pagina": pag})
        if resp_pag and resp_pag.get("data"):
            registros.extend(resp_pag["data"])

    return registros, True


def main():
    t0 = time.time()
    print("=" * 65)
    print(" PNCP Collector - Licitações PB (Lei 14.133/2021)")
    print(f" Range: {DATA_INICIO} -> {DATA_FIM}")
    print("=" * 65)

    meses        = gerar_meses(DATA_INICIO, DATA_FIM)
    total_blocos = len(meses) * len(MODALIDADES)
    print(f"\n[INFO] Grid: {len(meses)} meses x {len(MODALIDADES)} modalidades = {total_blocos} blocos\n")

    # ── Checkpoint Control ────────────────────────────────────────────────
    snap_jsonl = RAW_DIR / "pncp_parcial.jsonl"
    chaves_feitas: set[str] = set()

    # Recupera estado anterior, se existente
    if snap_jsonl.exists():
        with open(snap_jsonl, "r", encoding="utf-8") as fj:
            for linha in fj:
                try:
                    obj = json.loads(linha)
                    chave = obj.get("_chave", "")
                    if chave:
                        chaves_feitas.add(chave)
                except Exception:
                    pass
        if chaves_feitas:
            print(f"  [INFO] Retomando execução: {len(chaves_feitas)} blocos processados encontrados.\n")

    total = 0

    # ── Data Ingestion ────────────────────────────────────────────────────
    with open(snap_jsonl, "a", encoding="utf-8") as fj:
        for cod_mod, nome_mod in MODALIDADES.items():
            print(f"\n▶ [{cod_mod:02d}] {nome_mod}")

            for data_ini, data_fim in meses:
                chave = f"{cod_mod}_{data_ini.strftime('%Y-%m')}"

                if chave in chaves_feitas:
                    continue

                print(f"  {data_ini.strftime('%m/%Y')} ->", end=" ", flush=True)

                meta = {
                    "_chave":           chave,
                    "_modalidade":      cod_mod,
                    "_modalidade_nome": nome_mod,
                    "_mes":             data_ini.strftime("%Y-%m"),
                }

                registros, sucesso = coletar_bloco(cod_mod, data_ini, data_fim)

                if not sucesso:
                    print("[ERROR] Falha de comunicação. Bloco pendente para próxima execução.")
                    time.sleep(5)
                    continue

                if registros:
                    for rec in registros:
                        rec.update(meta)
                        fj.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    total += len(registros)
                    print(f"✓ (+{len(registros)})")
                else:
                    fj.write(json.dumps({**meta, "_sem_dados": True}) + "\n")
                    print("∅")

                fj.flush()
                time.sleep(SLEEP_MES)

            time.sleep(SLEEP_MODALIDADE)

    # ── Consolidação de Dados (JSONL -> CSV) ──────────────────────────────
    print(f"\n\n[INFO] Iniciando consolidação JSONL -> CSV...")
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
        print("[WARNING] Nenhum dado disponível para consolidação.")
        return

    df = pd.DataFrame(linhas)

    # Normalização de sub-estruturas (Flattening)
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

    # Seleção final de features
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
    
    # Validação de colunas existentes antes do subsetting
    df_out = df[[c for c in KEEP if c in df.columns]].copy()

    # Exportação
    df_out.to_csv(OUT_PROC, index=False, encoding="utf-8-sig")
    
    snap_csv = RAW_DIR / f"pncp_snapshot_{HOJE}.csv"
    df_out.to_csv(snap_csv, index=False, encoding="utf-8-sig")

    elapsed = time.time() - t0
    muns = df_out["municipio_ibge"].replace("", pd.NA).nunique()
    
    print(f"\n[SUCCESS] Pipeline concluído em {elapsed / 60:.1f} min")
    print(f"   Registros consolidados: {len(df_out):,}")
    print(f"   Municípios distintos:   {muns}")
    print(f"   Modalidades ativas:     {df_out['_modalidade'].nunique()}")
    print(f"   Arquivo primário:       {OUT_PROC.name}")
    print("=" * 65)

if __name__ == "__main__":
    main()