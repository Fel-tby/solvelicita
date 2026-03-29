"""
Coletor de Precatórios do TJ-PB — SolveLicita | Fase 2 (Validação)
Lê a documentação OpenAPI do TJ-PB, mapeia os IDs das entidades,
e varre a fila de pagamentos para somar o valor atualizado da dívida.
"""

import httpx
import pandas as pd
import time
import logging
import unicodedata
from pathlib import Path
from collections import defaultdict

# ── Configuração ──────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent.parent
PROCESSED = BASE_DIR / "data" / "processed"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# URLs mapeadas através do Swagger (OpenAPI 3.0.1) do TJ-PB
URL_ENTIDADES = "https://app.tjpb.jus.br/transparencia-precatorios/entidades"
URL_CONSULTA  = "https://app.tjpb.jus.br/transparencia-precatorios/consulta"

DELAY = 0.5

# ── Funções de Normalização ───────────────────────────────────────────────────

def normalizar_nome(texto: str) -> str:
    """
    Remove acentos, põe em maiúscula e tira prefixos jurídicos
    para batermos o nome da nossa tabela com o banco do TJ-PB.
    """
    if not isinstance(texto, str):
        return ""
    
    # Remove acentos
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    texto = texto.upper().strip()
    
    # Limpa prefixos
    prefixos = ["MUNICIPIO DE ", "PREFEITURA MUNICIPAL DE ", "PREFEITURA DE "]
    for p in prefixos:
        if texto.startswith(p):
            texto = texto.replace(p, "", 1)
            break
            
    return texto.strip()

# ── Extração do Dicionário de Entidades ───────────────────────────────────────

def obter_mapa_entidades(client: httpx.Client) -> dict:
    """
    Bate na rota /entidades e cria um dicionário:
    {'SOUSA': 153, 'JOAO PESSOA': 12, ...}
    """
    log.info("Buscando dicionário de Entidades (IDs) no TJ-PB...")
    r = client.get(URL_ENTIDADES, timeout=30)
    r.raise_for_status()
    
    lista_entidades = r.json()
    mapa = {}
    
    for ent in lista_entidades:
        id_tj = ent.get("id")
        nome_tj = ent.get("nome", "")
        nome_norm = normalizar_nome(nome_tj)
        mapa[nome_norm] = id_tj
        
    log.info(f"  {len(mapa)} entidades mapeadas com sucesso.")
    return mapa

# ── Coleta Paginada da Fila de Precatórios ────────────────────────────────────

def coletar_precatorios(municipios: pd.DataFrame) -> pd.DataFrame:
    registros = []
    
    with httpx.Client(verify=False, timeout=30.0) as client:
        # 1. Pega o dicionário de IDs
        mapa_entidades = obter_mapa_entidades(client)
        
        total_muns = len(municipios)
        
        for idx, mun in municipios.iterrows():
            cod  = str(mun["cod_ibge"])
            nome = mun["ente"]
            nome_norm = normalizar_nome(nome)
            
            entidade_id = mapa_entidades.get(nome_norm)
            
            if not entidade_id:
                # O município pode não ter precatórios ou ter nome muito diferente
                log.warning(f"[{idx+1:3d}/{total_muns}] ⚠️ {nome} não encontrado no TJ-PB. Ignorando.")
                continue
                
            log.info(f"[{idx+1:3d}/{total_muns}] Extraindo precatórios de {nome} (ID: {entidade_id})...")
            
            offset = 0
            limit = 50
            total_divida = 0.0
            qtd_processos = 0
            incidencia_ano = defaultdict(int)
            
            while True:
                params = {
                    "entidadeId": entidade_id,
                    "offset": offset,
                    "limit": limit
                }
                
                try:
                    r = client.get(URL_CONSULTA, params=params)
                    r.raise_for_status()
                    dados = r.json()
                except Exception as e:
                    log.error(f"Erro ao buscar {nome} na página offset={offset}: {e}")
                    break
                    
                items = dados.get("content", [])
                
                for item in items:
                    qtd_processos += 1
                    # Pega o valor corrigido pela inflação. Se faltar, pega o originário.
                    valor = item.get("valorAtual") or item.get("valorOriginario") or 0.0
                    total_divida += float(valor)
                    
                    ano = item.get("anoOrcamento", "Desc")
                    incidencia_ano[str(ano)] += 1

                # O Swagger diz que a resposta tem a chave 'last' indicando a última página
                is_last = dados.get("last", True)
                if is_last or not items:
                    break
                    
                offset += limit
                time.sleep(DELAY) # Respeito ao TJ
                
            # Formata anos para string legível
            str_anos = " | ".join([f"{a}: {q}" for a, q in sorted(incidencia_ano.items(), reverse=True)])
            
            registros.append({
                "cod_ibge": cod,
                "ente": nome,
                "tj_id": entidade_id,
                "qtd_precatorios": qtd_processos,
                "total_divida_tj": round(total_divida, 2),
                "incidencia_anos": str_anos
            })
            
            time.sleep(DELAY)

    return pd.DataFrame(registros)

# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Carregando tabela de municípios...")
    df_muns = pd.read_csv(PROCESSED / "municipios_pb_tabela.csv", dtype={"cod_ibge": str})
    
    log.info("Iniciando varredura oficial de dívidas ativas no TJ-PB...")
    df_prec = coletar_precatorios(df_muns)
    
    if not df_prec.empty:
        # Ordena do maior devedor pro menor
        df_prec = df_prec.sort_values(by="total_divida_tj", ascending=False)
        
        arquivo_saida = PROCESSED / "precatorios_pb.csv"
        df_prec.to_csv(arquivo_saida, index=False, encoding="utf-8-sig")
        log.info(f"\n✅ Coleta concluída! Salvo em: {arquivo_saida}")
        
        # O momento da verdade: Top 5 devedores
        log.info("\n🏆 TOP 5 Maiores Devedores de Precatórios na PB:")
        cols_print = ["ente", "total_divida_tj", "qtd_precatorios"]
        
        # Formata o dinheiro para ficar legível no terminal
        df_print = df_prec.head(5).copy()
        df_print["total_divida_tj"] = df_print["total_divida_tj"].apply(lambda x: f"R$ {x:,.2f}")
        print(df_print[cols_print].to_string(index=False))
    else:
        log.error("Nenhum dado retornado.")