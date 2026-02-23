import asyncio
import httpx
import pandas as pd
import time
from pathlib import Path

# Configuração de Diretórios
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw" / "siconfi"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL_SICONFI = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"

# Limitador de concorrência: máximo de 20 requisições simultâneas
MAX_CONCORRENCIA = 20
semaforo = asyncio.Semaphore(MAX_CONCORRENCIA)

def obter_municipios_pb() -> list:
    url_ibge = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/PB/municipios"
    print("Buscando malha de municípios no IBGE...")
    try:
        resposta = httpx.get(url_ibge, timeout=15)
        resposta.raise_for_status()
        municipios = [str(mun["id"]) for mun in resposta.json()]
        print(f"Sucesso: {len(municipios)} municípios na Paraíba.\n")
        return municipios
    except Exception as e:
        print(f"Erro ao buscar IBGE: {e}")
        return ["2504009", "2507507"] 

async def fetch_com_retry(client: httpx.AsyncClient, url: str, params: dict, tentativas: int = 3):
    """
    Faz a requisição com tentativas embutidas (Exponential Backoff).
    Ideal para lidar com instabilidades em APIs governamentais.
    """
    for tentativa in range(tentativas):
        try:
            resposta = await client.get(url, params=params)
            
            # Se a requisição for muito rápida, o servidor pode rejeitar (429)
            if resposta.status_code == 429:
                espera = 2 ** tentativa
                await asyncio.sleep(espera)
                continue
                
            resposta.raise_for_status()
            return resposta.json()
        
        except httpx.HTTPStatusError as e:
            # Siconfi costuma retornar 400 se o ente não entregou o balanço
            if e.response.status_code in (400, 404):
                return None 
            await asyncio.sleep(2 ** tentativa)
            
        except httpx.RequestError:
            await asyncio.sleep(2 ** tentativa)
            
    return None

async def extrair_assincrono(client: httpx.AsyncClient, endpoint: str, ano: int, periodo: int, id_ente: str, anexo: str, poder: str = None) -> list:
    url = f"{BASE_URL_SICONFI}/{endpoint}"
    params = {
        "an_exercicio": ano,
        "nr_periodo": periodo,
        "co_tipo_demonstrativo": endpoint.upper(),
        "no_anexo": anexo,
        "id_ente": id_ente
    }
    if poder and endpoint == 'rgf':
        params["co_poder"] = poder

    todos_registros = []
    offset = 0
    limit = 5000

    async with semaforo:  # Garante que só MAX_CONCORRENCIA rodem ao mesmo tempo
        while True:
            params["offset"] = offset
            dados = await fetch_com_retry(client, url, params)
            
            if not dados or "items" not in dados:
                break
                
            items = dados.get("items", [])
            todos_registros.extend(items)
            
            if not dados.get("hasMore", False):
                break
                
            offset += limit
            
    return todos_registros

async def orquestrar_coleta_assincrona():
    inicio = time.time()
    print("=== Iniciando Crawler SICONFI de Alta Performance ===")
    
    municipios_pb = obter_municipios_pb()
    anos = [2021, 2022, 2023, 2024, 2025, 2026]
    
    # Criamos o cliente assíncrono com pool de conexões otimizado
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=30)
    
    async with httpx.AsyncClient(timeout=45.0, limits=limits) as client:
        tarefas_rreo = []
        tarefas_rgf = []
        
        print("Montando as milhares de requisições na memória...")
        for ano in anos:
            for id_ente in municipios_pb:
                # Monta as tarefas do RREO
                for periodo_rreo in range(1, 7):
                    tarefas_rreo.append(
                        extrair_assincrono(client, "rreo", ano, periodo_rreo, id_ente, "RREO-Anexo 07")
                    )
                # Monta as tarefas do RGF
                for periodo_rgf in range(1, 4):
                    tarefas_rgf.append(
                        extrair_assincrono(client, "rgf", ano, periodo_rgf, id_ente, "RGF-Anexo 05", poder="E")
                    )
                    
        print(f"Total de {len(tarefas_rreo)} requisições RREO e {len(tarefas_rgf)} requisições RGF agendadas.")
        print("Disparando requisições em paralelo... (Isso pode levar alguns minutos, dependendo da rede)")
        
        # Executa tudo de forma concorrente
        resultados_rreo = await asyncio.gather(*tarefas_rreo)
        resultados_rgf = await asyncio.gather(*tarefas_rgf)

    # Nivelando (flattening) as listas de listas que o gather retorna
    registros_rreo = [item for sublist in resultados_rreo for item in sublist if sublist]
    registros_rgf = [item for sublist in resultados_rgf for item in sublist if sublist]

    if registros_rreo:
        df_rreo = pd.DataFrame(registros_rreo)
        df_rreo.to_csv(PROCESSED_DIR / "siconfi_rreo_pb_rap.csv", index=False)
        print(f"\nRREO: {len(df_rreo)} linhas estruturadas com sucesso.")

    if registros_rgf:
        df_rgf = pd.DataFrame(registros_rgf)
        df_rgf.to_csv(PROCESSED_DIR / "siconfi_rgf_pb_rap.csv", index=False)
        print(f"RGF: {len(df_rgf)} linhas estruturadas com sucesso.")

    fim = time.time()
    minutos = (fim - inicio) / 60
    print(f"\n=== Coleta concluída em {minutos:.2f} minutos! ===")

if __name__ == "__main__":
    # Roda o event loop assíncrono
    asyncio.run(orquestrar_coleta_assincrona())