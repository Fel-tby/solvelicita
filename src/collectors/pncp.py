"""
Coletor PNCP — Licitações por UF (Lei 14.133/2021).
Responsabilidade: coletar registros paginados por modalidade/mês e
salvar o JSONL de checkpoint incremental.

A consolidação JSONL → CSV, flattening de campos aninhados e seleção
de colunas é feita por:
    src/processors/pncp_processor.py

Rodar individualmente:
    python src/collectors/pncp.py                      # full PB (desde 2023-01-01)
    python src/collectors/pncp.py --mode incremental   # apenas últimos 2 meses
    python src/collectors/pncp.py --uf CE              # outro estado, full
    python src/collectors/pncp.py --mode incremental --uf CE
"""

import sys
import json
import time
import requests
from pathlib import Path
from datetime import date, timedelta
from calendar import monthrange

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.paths import get_paths, RAW
from utils.bigquery_loader import upload_raw

BASE_URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"

# Full: desde o início da vigência da lei (nacional — não varia por UF).
DATA_INICIO_FULL       = date(2023, 1, 1)
JANELA_INCREMENTAL_DIAS = 60

MODALIDADES = {
    1 : "Leilão Eletrônico",
    2 : "Diálogo Competitivo",
    3 : "Concurso",
    4 : "Concorrência Eletrônica",
    5 : "Concorrência Presencial",
    6 : "Pregão Eletrônico",
    7 : "Pregão Presencial",
    8 : "Dispensa de Licitação",
    9 : "Inexigibilidade",
    10: "Manifestação de Interesse",
    11: "Pré-qualificação",
    12: "Credenciamento",
    13: "Leilão Presencial",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept"    : "application/json",
}

TAMANHO_PAGINA    = 50
SLEEP_PAGINA      = 0.5
SLEEP_MES         = 0.4
SLEEP_MODALIDADE  = 2.0
BACKOFF_429       = 60
MAX_RETRIES       = 6


def gerar_meses(inicio: date, fim: date) -> list[tuple[date, date]]:
    meses = []
    ano, mes = inicio.year, inicio.month
    while date(ano, mes, 1) <= fim:
        ultimo_dia = monthrange(ano, mes)[1]
        fim_mes    = min(date(ano, mes, ultimo_dia), fim)
        meses.append((date(ano, mes, 1), fim_mes))
        mes += 1
        if mes > 12:
            mes  = 1
            ano += 1
    return meses


def fetch_com_backoff(params: dict) -> dict | None:
    for t in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 204:
                return {"data": [], "totalRegistros": 0, "totalPaginas": 0, "empty": True}
            if r.status_code == 429:
                espera = BACKOFF_429 * t
                print(f"\n  [WARNING] 429 Rate Limit. Aguardando {espera}s (Tentativa {t}/{MAX_RETRIES})")
                time.sleep(espera)
                continue
            if r.status_code in (500, 502, 503, 504):
                time.sleep(15 * t)
                continue
            print(f"\n  [ERROR] HTTP {r.status_code}: {r.text[:120]}")
            return None
        except requests.exceptions.Timeout:
            time.sleep(10 * t)
        except requests.exceptions.ConnectionError:
            time.sleep(20 * t)
    print(f"\n  [ERROR] Falha definitiva após {MAX_RETRIES} tentativas.")
    return None


def coletar_bloco(
    modalidade: int,
    data_ini:   date,
    data_fim:   date,
    uf:         str,
) -> tuple[list, bool]:
    params_base = {
        "dataInicial"                  : data_ini.strftime("%Y%m%d"),
        "dataFinal"                    : data_fim.strftime("%Y%m%d"),
        "codigoModalidadeContratacao"  : modalidade,
        "uf"                           : uf.upper(),
        "tamanhoPagina"                : TAMANHO_PAGINA,
        "pagina"                       : 1,
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
        print(f"  {total_regs} regs / {total_pags} págs", end="", flush=True)

    for pag in range(2, total_pags + 1):
        time.sleep(SLEEP_PAGINA)
        resp_pag = fetch_com_backoff({**params_base, "pagina": pag})
        if resp_pag and resp_pag.get("data"):
            registros.extend(resp_pag["data"])

    return registros, True


def run(mode: str = "full", uf: str = "PB") -> None:
    """
    Executa a coleta PNCP e salva o JSONL de checkpoint.

    Parâmetros
    ----------
    mode : "full"        — coleta desde DATA_INICIO_FULL (2023-01-01).
                           Se o JSONL já existir, apaga e reinicia do zero.
           "incremental" — coleta apenas os últimos JANELA_INCREMENTAL_DIAS dias.
                           Usa o checkpoint JSONL existente (pula blocos já feitos).
    uf   : sigla do estado (default "PB")
    """
    uf    = uf.upper()
    hoje  = date.today()
    paths = get_paths(uf)

    # Primário — nova estrutura UF subfolder
    snap_jsonl_novo = paths["raw_pncp"] / f"pncp_parcial_{uf.lower()}.jsonl"

    # Legacy flat — pncp_processor.py lê daqui até o Bloco 9
    raw_pncp_flat    = RAW / "pncp"
    raw_pncp_flat.mkdir(parents=True, exist_ok=True)
    snap_jsonl_compat = raw_pncp_flat / "pncp_parcial.jsonl"

    if mode == "full":
        data_inicio = DATA_INICIO_FULL
        for jsonl in (snap_jsonl_novo, snap_jsonl_compat):
            if jsonl.exists():
                jsonl.unlink()
                print(f"  [INFO] {jsonl.name} removido — coleta full reiniciada.")
    else:
        data_inicio = hoje - timedelta(days=JANELA_INCREMENTAL_DIAS)

    t0    = time.time()
    meses = gerar_meses(data_inicio, hoje)

    print("=" * 65)
    print(f"  PNCP Collector — Licitações {uf} (Lei 14.133/2021)")
    print(f"  Modo   : {mode.upper()}")
    print(f"  Range  : {data_inicio} → {hoje}")
    print(f"  Janela : {len(meses)} meses × {len(MODALIDADES)} modalidades = {len(meses)*len(MODALIDADES)} blocos")
    print("=" * 65)

    # Checkpoint: blocos já coletados (lê do primário)
    chaves_feitas: set[str] = set()
    if snap_jsonl_novo.exists():
        with open(snap_jsonl_novo, "r", encoding="utf-8") as fj:
            for linha in fj:
                try:
                    obj = json.loads(linha)
                    if obj.get("_chave"):
                        chaves_feitas.add(obj["_chave"])
                except Exception:
                    pass
        if chaves_feitas:
            print(f"\n  [INFO] {len(chaves_feitas)} blocos já no JSONL — serão ignorados.")

    total = 0

    with (
        open(snap_jsonl_novo,   "a", encoding="utf-8") as fj_novo,
        open(snap_jsonl_compat, "a", encoding="utf-8") as fj_compat,
    ):
        for cod_mod, nome_mod in MODALIDADES.items():
            print(f"\n▶ [{cod_mod:02d}] {nome_mod}")

            for data_ini, data_fim_bloco in meses:
                chave = f"{cod_mod}_{data_ini.strftime('%Y-%m')}"

                if chave in chaves_feitas:
                    continue

                print(f"  {data_ini.strftime('%m/%Y')} →", end=" ", flush=True)

                meta = {
                    "_chave"          : chave,
                    "_modalidade"     : cod_mod,
                    "_modalidade_nome": nome_mod,
                    "_mes"            : data_ini.strftime("%Y-%m"),
                    "_uf"             : uf,
                }

                registros, sucesso = coletar_bloco(cod_mod, data_ini, data_fim_bloco, uf)

                if not sucesso:
                    print("[ERROR] Falha de comunicação. Bloco pendente.")
                    time.sleep(5)
                    continue

                if registros:
                    for rec in registros:
                        rec.update(meta)
                        linha_json = json.dumps(rec, ensure_ascii=False) + "\n"
                        fj_novo.write(linha_json)
                        fj_compat.write(linha_json)
                    total += len(registros)
                    print(f"✓ (+{len(registros)})")
                else:
                    vazio = json.dumps({**meta, "_sem_dados": True}, ensure_ascii=False) + "\n"
                    fj_novo.write(vazio)
                    fj_compat.write(vazio)
                    print("∅")

                fj_novo.flush()
                fj_compat.flush()
                time.sleep(SLEEP_MES)

            time.sleep(SLEEP_MODALIDADE)

    elapsed = time.time() - t0
    print(f"\n[SUCCESS] Coleta concluída em {elapsed / 60:.1f} min")
    print(f"  Registros novos : {total:,}")
    print(f"  JSONL primário  : {snap_jsonl_novo.name}")
    print(f"  JSONL compat    : {snap_jsonl_compat.name}")
    print(f"  Próximo passo   : python src/processors/pncp_processor.py")
    print("=" * 65)

    # BigQuery: não há DataFrame disponível aqui (JSONL processado pelo processor).
    # O upload para raw.pncp_licitacoes é feito pelo pncp_processor após flatten.


if __name__ == "__main__":
    args      = sys.argv[1:]
    mode_arg  = "full"
    uf_arg    = "PB"
    for i, arg in enumerate(args):
        if arg == "--mode" and i + 1 < len(args):
            mode_arg = args[i + 1]
        elif arg.startswith("--mode="):
            mode_arg = arg.split("=", 1)[1]
        elif arg == "--uf" and i + 1 < len(args):
            uf_arg = args[i + 1]
        elif arg.startswith("--uf="):
            uf_arg = arg.split("=", 1)[1]
    run(mode=mode_arg, uf=uf_arg)