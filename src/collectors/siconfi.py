import asyncio
import httpx
import pandas as pd
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw" / "siconfi"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL_SICONFI = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"

MAX_CONCORRENCIA = 10
semaforo = asyncio.Semaphore(MAX_CONCORRENCIA)

# Anexos coletados e o que cada um representa no score
ANEXOS_RREO = [
    "RREO-Anexo 01",  # Balanço Orçamentário → variável Eorcam (peso 25%)
    "RREO-Anexo 07",  # Restos a Pagar       → variável Rrestos (peso 20%)
]

ANEXOS_RGF = [
    "RGF-Anexo 05",   # Restos a Pagar do RGF (perspectiva quadrimestral)
]


def obter_municipios_pb() -> list:
    url_ibge = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/PB/municipios"
    print("Buscando municípios da PB no IBGE...")
    try:
        resposta = httpx.get(url_ibge, timeout=15)
        resposta.raise_for_status()
        municipios = [str(mun["id"]) for mun in resposta.json()]
        print(f"  {len(municipios)} municípios encontrados.\n")
        return municipios
    except Exception as e:
        print(f"  Erro ao buscar IBGE: {e}")
        # Fallback: Campina Grande + João Pessoa para não travar tudo
        return ["2504100", "2507507"]


async def fetch_com_retry(
    client: httpx.AsyncClient,
    url: str,
    params: dict,
    tentativas: int = 3
):
    for tentativa in range(tentativas):
        try:
            resposta = await client.get(url, params=params)

            if resposta.status_code == 429:
                await asyncio.sleep(2 ** tentativa)
                continue

            resposta.raise_for_status()
            return resposta.json()

        except httpx.HTTPStatusError as e:
            # 400/404 = município não entregou o relatório naquele período
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
    poder: str = None
) -> list:
    url = f"{BASE_URL_SICONFI}/{endpoint}"
    params = {
        "an_exercicio": ano,
        "nr_periodo": periodo,
        "co_tipo_demonstrativo": endpoint.upper(),
        "no_anexo": anexo,
        "id_ente": id_ente,
        "offset": 0,
        "limit": 5000,
    }
    if poder and endpoint == "rgf":
        params["co_poder"] = poder

    todos_registros = []
    offset = 0

    async with semaforo:
        while True:
            params["offset"] = offset
            dados = await fetch_com_retry(client, url, params)

            if not dados or "items" not in dados:
                break

            items = dados.get("items", [])
            todos_registros.extend(items)

            if not dados.get("hasMore", False):
                break

            offset += 5000

    return todos_registros


async def orquestrar_coleta():
    inicio = time.time()
    print("=" * 55)
    print("  Crawler SICONFI — Municípios da Paraíba")
    print("=" * 55)

    municipios_pb = obter_municipios_pb()

    # Mantém até 2026: quando novos dados forem publicados, já são coletados
    anos = [2020, 2021, 2022, 2023, 2024, 2025, 2026]

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=15)

    async with httpx.AsyncClient(timeout=45.0, limits=limits) as client:
        tarefas_rreo = []
        tarefas_rgf = []

        print("Montando tarefas...")
        for ano in anos:
            for id_ente in municipios_pb:
                # RREO: bimestral (6 períodos/ano), todos os anexos necessários
                for periodo in range(1, 7):
                    for anexo in ANEXOS_RREO:
                        tarefas_rreo.append(
                            extrair_assincrono(
                                client, "rreo", ano, periodo, id_ente, anexo
                            )
                        )

                # RGF: quadrimestral (3 períodos/ano)
                for periodo in range(1, 4):
                    for anexo in ANEXOS_RGF:
                        tarefas_rgf.append(
                            extrair_assincrono(
                                client, "rgf", ano, periodo, id_ente, anexo, poder="E"
                            )
                        )

        total = len(tarefas_rreo) + len(tarefas_rgf)
        print(f"  {len(tarefas_rreo)} tarefas RREO")
        print(f"  {len(tarefas_rgf)} tarefas RGF")
        print(f"  {total} requisições no total\n")
        print("Executando em paralelo (isso pode levar alguns minutos)...")

        resultados_rreo = await asyncio.gather(*tarefas_rreo)
        resultados_rgf = await asyncio.gather(*tarefas_rgf)

    # Flatten: asyncio.gather retorna lista de listas
    registros_rreo = [
        item for sublist in resultados_rreo
        if sublist for item in sublist
    ]
    registros_rgf = [
        item for sublist in resultados_rgf
        if sublist for item in sublist
    ]

    # Salva RREO
    if registros_rreo:
        df_rreo = pd.DataFrame(registros_rreo)
        caminho_rreo = PROCESSED_DIR / "siconfi_rreo_pb.csv"
        df_rreo.to_csv(caminho_rreo, index=False, encoding="utf-8")
        print(f"\n✅ RREO salvo: {len(df_rreo):,} linhas → {caminho_rreo.name}")
        print(f"   Colunas: {list(df_rreo.columns)}")
        print(f"   Anos com dados: {sorted(df_rreo['exercicio'].unique())}")
        print(f"   Anexos coletados: {sorted(df_rreo['anexo'].unique())}")
    else:
        print("\n⚠️  RREO: nenhum dado retornado.")

    # Salva RGF
    if registros_rgf:
        df_rgf = pd.DataFrame(registros_rgf)
        caminho_rgf = PROCESSED_DIR / "siconfi_rgf_pb.csv"
        df_rgf.to_csv(caminho_rgf, index=False, encoding="utf-8")
        print(f"\n✅ RGF salvo: {len(df_rgf):,} linhas → {caminho_rgf.name}")
        print(f"   Anos com dados: {sorted(df_rgf['exercicio'].unique())}")
    else:
        print("\n⚠️  RGF: nenhum dado retornado.")

    fim = time.time()
    print(f"\n⏱  Concluído em {(fim - inicio) / 60:.1f} minutos.")
    print("=" * 55)


if __name__ == "__main__":
    asyncio.run(orquestrar_coleta())
