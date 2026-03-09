"""
Crawler assíncrono para extração de relatórios contábeis do SICONFI.
Gerencia requisições concorrentes aos endpoints RREO e RGF, implementando
semáforos de controle de tráfego e resiliência contra rate limits (429).

Output (bruto):
    raw/siconfi/siconfi_rreo_pb.csv
    raw/siconfi/siconfi_rgf_pb.csv

O processamento analítico dos arquivos brutos é feito por:
    src/processors/siconfi_processor.py

Rodar individualmente:
    python src/collectors/siconfi.py                     # full (2020–hoje)
    python src/collectors/siconfi.py --mode incremental  # apenas anos recentes
"""

import asyncio
import httpx
import logging
import pandas as pd
import sys
import time
from datetime import date
from pathlib import Path

# ── Silencia loggers verbosos de libs externas ────────────────────────────────
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ── Diretórios ────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent.parent
RAW_DIR   = BASE_DIR / "data" / "raw" / "siconfi"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Caminhos legados (código anterior salvava em processed/ diretamente).
# Usados como fallback na primeira execução após a refatoração.
LEGACY_RREO = BASE_DIR / "data" / "processed" / "siconfi_rreo_pb.csv"
LEGACY_RGF  = BASE_DIR / "data" / "processed" / "siconfi_rgf_pb.csv"

# ── API e concorrência ────────────────────────────────────────────────────────
BASE_URL_SICONFI = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"
MAX_CONCORRENCIA = 10

ANOS_FULL        = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
ANOS_INCREMENTAL = [date.today().year - 1, date.today().year]

ANEXOS_RREO = ["RREO-Anexo 01", "RREO-Anexo 07"]
ANEXOS_RGF  = ["RGF-Anexo 05"]

CHAVE_RREO = ["cod_ibge", "exercicio", "periodo", "anexo", "cod_conta", "coluna"]
CHAVE_RGF  = ["cod_ibge", "exercicio", "periodo", "anexo", "periodicidade", "cod_conta", "coluna"]


# ── Progress display ──────────────────────────────────────────────────────────

class Progresso:
    """Contador thread-safe com exibição em linha única."""

    def __init__(self, total: int, label: str = ""):
        self.total     = total
        self.label     = label
        self.feitas    = 0
        self.erros     = 0
        self.vazias    = 0
        self.registros = 0
        self._lock     = asyncio.Lock()
        self._inicio   = time.time()

    async def tick(self, n_registros: int = 0, erro: bool = False, vazia: bool = False):
        async with self._lock:
            self.feitas += 1
            self.registros += n_registros
            if erro:
                self.erros += 1
            if vazia:
                self.vazias += 1
            self._render()

    def _render(self):
        elapsed = time.time() - self._inicio
        pct     = self.feitas / self.total * 100
        bar_len = 30
        filled  = int(bar_len * self.feitas / self.total)
        bar     = "█" * filled + "░" * (bar_len - filled)
        eta     = (elapsed / self.feitas * (self.total - self.feitas)) if self.feitas else 0
        eta_str = f"{eta/60:.0f}min" if eta >= 60 else f"{eta:.0f}s"

        line = (
            f"\r  {self.label} [{bar}] "
            f"{self.feitas:,}/{self.total:,} ({pct:.1f}%) | "
            f"registros: {self.registros:,} | "
            f"erros: {self.erros} | "
            f"ETA: {eta_str}   "
        )
        sys.stdout.write(line)
        sys.stdout.flush()

    def finalizar(self):
        elapsed = time.time() - self._inicio
        sys.stdout.write("\n")
        sys.stdout.flush()
        print(f"  ✅ {self.label}: {self.registros:,} registros em {elapsed/60:.1f} min")


# ── HTTP ──────────────────────────────────────────────────────────────────────

def obter_municipios_pb() -> list:
    url_ibge = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/PB/municipios"
    print("  Buscando municípios da PB no IBGE...", end=" ")
    try:
        resposta = httpx.get(url_ibge, timeout=15)
        resposta.raise_for_status()
        municipios = [str(mun["id"]) for mun in resposta.json()]
        print(f"{len(municipios)} encontrados.")
        return municipios
    except Exception as e:
        print(f"\n  Erro ao buscar IBGE: {e}")
        return ["2504100", "2507507"]


async def fetch_com_retry(
    client: httpx.AsyncClient,
    url: str,
    params: dict,
    pausa_global: asyncio.Event,
    tentativas: int = 3,
) -> dict | None:
    for tentativa in range(tentativas):
        await pausa_global.wait()
        try:
            resposta = await client.get(url, params=params)

            if resposta.status_code == 429:
                pausa_global.clear()
                await asyncio.sleep(2 ** tentativa)
                pausa_global.set()
                continue

            resposta.raise_for_status()
            return resposta.json()

        except httpx.HTTPStatusError as e:
            if e.response.status_code in (400, 404):
                return None
            await asyncio.sleep(2 ** tentativa)

        except httpx.RequestError:
            await asyncio.sleep(2 ** tentativa)

    return None


async def extrair_assincrono(
    client: httpx.AsyncClient,
    endpoint: str,
    ano: int,
    periodo: int,
    id_ente: str,
    anexo: str,
    semaforo: asyncio.Semaphore,
    pausa_global: asyncio.Event,
    progresso: Progresso,
    poder: str = None,
    periodicidade: str = None,
) -> list:
    url    = f"{BASE_URL_SICONFI}/{endpoint}"
    params = {
        "an_exercicio":          ano,
        "nr_periodo":            periodo,
        "co_tipo_demonstrativo": endpoint.upper(),
        "no_anexo":              anexo,
        "id_ente":               id_ente,
        "offset":                0,
        "limit":                 5000,
    }
    if poder and endpoint == "rgf":
        params["co_poder"] = poder
    if periodicidade:
        params["in_periodicidade"] = periodicidade

    todos_registros = []
    offset          = 0
    erro            = False

    async with semaforo:
        while True:
            params["offset"] = offset
            dados = await fetch_com_retry(client, url, params, pausa_global)

            if dados is None:
                erro = True
                break

            if "items" not in dados:
                break

            todos_registros.extend(dados.get("items", []))

            if not dados.get("hasMore", False):
                break

            offset += 5000

    await progresso.tick(
        n_registros=len(todos_registros),
        erro=erro,
        vazia=(len(todos_registros) == 0 and not erro),
    )
    return todos_registros


# ── Persistência ──────────────────────────────────────────────────────────────

def _carregar_base(caminho_novo: Path, caminho_legado: Path) -> pd.DataFrame | None:
    """
    Carrega o DataFrame base para merge incremental.

    Prioridade:
      1. caminho_novo  (raw/siconfi/) — arquivo já na estrutura nova
      2. caminho_legado (processed/)  — arquivo da estrutura anterior à refatoração
      3. None                          — primeira coleta, sem histórico

    Se encontrar apenas o legado, avisa e usa como base (sem mover o arquivo —
    o usuário pode apagar o legado após confirmar que o novo está correto).
    """
    if caminho_novo.exists():
        return pd.read_csv(caminho_novo, dtype=str)

    if caminho_legado.exists():
        print(f"\n  ⚠️  Arquivo novo não encontrado. Usando base legada: {caminho_legado.name}")
        print(f"      (após confirmar integridade, você pode remover {caminho_legado})")
        return pd.read_csv(caminho_legado, dtype=str)

    return None


def _salvar_com_merge(
    df_novo: pd.DataFrame,
    caminho: Path,
    chave: list[str],
    caminho_legado: Path,
) -> None:
    """
    Salva df_novo. Se existir base histórica (novo ou legado),
    concatena e desuplica pela chave natural — mantendo o registro mais recente.
    """
    df_existente = _carregar_base(caminho, caminho_legado)

    if df_existente is not None:
        df_final     = pd.concat([df_existente, df_novo.astype(str)], ignore_index=True)
        chave_valida = [c for c in chave if c in df_final.columns]
        if chave_valida:
            antes    = len(df_final)
            df_final = df_final.drop_duplicates(subset=chave_valida, keep="last")
            duplic   = antes - len(df_final)
            if duplic:
                print(f"\n  🔁 {duplic:,} duplicatas removidas no merge ({caminho.name})")
    else:
        df_final = df_novo

    df_final.to_csv(caminho, index=False, encoding="utf-8")
    anos = sorted(df_final["exercicio"].unique()) if "exercicio" in df_final.columns else []
    print(f"  💾 {caminho.name}: {len(df_final):,} linhas | anos: {anos}")


# ── Orquestração ──────────────────────────────────────────────────────────────

async def orquestrar_coleta(anos: list[int]) -> None:
    semaforo     = asyncio.Semaphore(MAX_CONCORRENCIA)
    pausa_global = asyncio.Event()
    pausa_global.set()

    inicio        = time.time()
    municipios_pb = obter_municipios_pb()
    limits        = httpx.Limits(max_keepalive_connections=10, max_connections=15)

    tarefas_rreo_params = []
    tarefas_rgf_params  = []

    for ano in anos:
        for id_ente in municipios_pb:
            for periodo in range(1, 7):
                for anexo in ANEXOS_RREO:
                    tarefas_rreo_params.append((ano, periodo, id_ente, anexo, None, None))
            for periodo in range(1, 4):
                for anexo in ANEXOS_RGF:
                    tarefas_rgf_params.append((ano, periodo, id_ente, anexo, "E", "Q"))
            for periodo in range(1, 3):
                for anexo in ANEXOS_RGF:
                    tarefas_rgf_params.append((ano, periodo, id_ente, anexo, "E", "S"))

    total_rreo = len(tarefas_rreo_params)
    total_rgf  = len(tarefas_rgf_params)

    print(f"\n  Malha montada:")
    print(f"    RREO : {total_rreo:,} requisições")
    print(f"    RGF  : {total_rgf:,} requisições")
    print(f"    Total: {total_rreo + total_rgf:,} requisições\n")

    prog_rreo = Progresso(total_rreo, "RREO")
    prog_rgf  = Progresso(total_rgf,  "RGF ")

    async with httpx.AsyncClient(timeout=45.0, limits=limits) as client:

        tarefas_rreo = [
            extrair_assincrono(
                client, "rreo", ano, periodo, id_ente, anexo,
                semaforo, pausa_global, prog_rreo,
            )
            for (ano, periodo, id_ente, anexo, _, __) in tarefas_rreo_params
        ]

        tarefas_rgf = [
            extrair_assincrono(
                client, "rgf", ano, periodo, id_ente, anexo,
                semaforo, pausa_global, prog_rgf,
                poder=poder, periodicidade=periodicidade,
            )
            for (ano, periodo, id_ente, anexo, poder, periodicidade) in tarefas_rgf_params
        ]

        resultados_rreo, resultados_rgf = await asyncio.gather(
            asyncio.gather(*tarefas_rreo),
            asyncio.gather(*tarefas_rgf),
        )

    prog_rreo.finalizar()
    prog_rgf.finalizar()
    print()

    registros_rreo = [item for sub in resultados_rreo if sub for item in sub]
    registros_rgf  = [item for sub in resultados_rgf  if sub for item in sub]

    if registros_rreo:
        _salvar_com_merge(
            pd.DataFrame(registros_rreo),
            RAW_DIR / "siconfi_rreo_pb.csv",
            CHAVE_RREO,
            LEGACY_RREO,
        )
    else:
        print("  ⚠️  RREO: nenhum dado retornado.")

    if registros_rgf:
        _salvar_com_merge(
            pd.DataFrame(registros_rgf),
            RAW_DIR / "siconfi_rgf_pb.csv",
            CHAVE_RGF,
            LEGACY_RGF,
        )
    else:
        print("  ⚠️  RGF: nenhum dado retornado.")

    print(f"\n  ⏱  Total: {(time.time() - inicio) / 60:.1f} minutos.")


# ── Entry points ──────────────────────────────────────────────────────────────

def run(mode: str = "full") -> None:
    anos = ANOS_FULL if mode == "full" else ANOS_INCREMENTAL
    print(f"\n{'='*55}")
    print(f"  Crawler SICONFI — Municípios da Paraíba")
    print(f"  Modo: {mode.upper()} | Anos: {anos}")
    print(f"{'='*55}")
    asyncio.run(orquestrar_coleta(anos))


if __name__ == "__main__":
    mode = "incremental" if "--mode" in sys.argv and "incremental" in sys.argv else "full"
    run(mode=mode)