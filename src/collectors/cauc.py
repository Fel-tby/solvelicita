"""
Módulo de extração de dados do CAUC via CKAN - Tesouro Transparente.
Realiza o download em bulk do relatório nacional de situação dos entes federados
e aplica filtragem geográfica para o estado da Paraíba.
"""

import pandas as pd
import requests
import io
from pathlib import Path
from datetime import date
import urllib3

# Desabilita alertas de requisições HTTPS sem verificação de certificado
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Configurações de Diretórios e Variáveis Globais ───────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR  = BASE_DIR / "data" / "raw" / "cauc"
OUT_PROC = BASE_DIR / "data" / "processed" / "cauc_situacao_pb.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

HOJE   = date.today().strftime("%Y-%m-%d")
TABELA = BASE_DIR / "data" / "processed" / "municipios_pb_tabela.csv"

# Endpoint do CKAN para download do dump completo de municípios
URL_CAUC_BULK = (
    "https://www.tesourotransparente.gov.br/ckan/dataset/"
    "72b5f371-0c35-4613-8076-c99c821a6410/resource/"
    "07af297a-5e59-494a-a88a-55ddfd2f4b01/download/"
    "relatorio-situacao-de-varios-entes---municipios---uf-todas---abrangencia-1.csv"
)

# Dicionário de mapeamento de requisitos de regularidade fiscal e previdenciária
REQUISITOS = {
    "1.1": "Regularidade Previdenciária (RPPS)",
    "1.2": "Regularidade Fiscal (RFB)",
    "1.3": "Regularidade PGFN",
    "1.4": "Regularidade FGTS",
    "1.5": "Regularidade Trabalhista (TST)",
    "2.1.1": "LRF - Limite Pessoal Executivo",
    "2.1.2": "LRF - Limite Pessoal Legislativo",
    "3.1.1": "SIOPS (Saúde)",
    "3.1.2": "SIOPS Demonstrativo",
    "3.2.1": "SIOPE (Educação)",
    "3.2.2": "SIOPE Demonstrativo",
    "3.2.3": "SIOPE Complementar",
    "3.2.4": "SIOPE Observações",
    "3.3": "SIGA (Alimentação Escolar)",
    "3.4.1": "SICONV/TRANSFEREGOV Prestação de Contas",
    "3.4.2": "SICONV/TRANSFEREGOV Débitos",
    "3.5": "CADIN",
    "3.6": "Adimplência TCU",
    "3.7": "Adimplência CGU",
    "4.1": "SISTN (Dívida Consolidada)",
    "4.2": "SISTN (Garantias)",
    "5.1": "SICONFI RREO",
    "5.2": "SICONFI RGF",
    "5.3": "SICONFI Balanço Anual",
    "5.4": "SICONFI DCA",
    "5.5": "SICONFI PCASP",
    "5.6": "SICONFI DCASP",
    "5.7": "SICONFI MCASP",
}

def main():
    """
    Função principal de orquestração da coleta do CAUC.
    Executa o pipeline de ingestão, tratamento de encoding, filtragem espacial
    e consolidação do status de bloqueio dos municípios.
    """
    print("=" * 70)
    print("  Coletor CAUC — CKAN Tesouro Transparente")
    print(f"  Execução: {HOJE}")
    print("=" * 70)

    # ── Ingestão da Malha Geográfica ──────────────────────────────────────────
    municipios_df = pd.read_csv(TABELA)
    municipios_df["cod_ibge"] = municipios_df["cod_ibge"].astype(str)

    print(f"\n  Baixando CSV nacional do CKAN...")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    resp = requests.get(URL_CAUC_BULK, headers=headers, verify=False, timeout=60)
    resp.raise_for_status()

    # ── Tratamento de Encoding e Parse do CSV ─────────────────────────────────
    try:
        texto = resp.content.decode("utf-8-sig")
    except UnicodeDecodeError:
        texto = resp.content.decode("iso-8859-1")

    # Ignora as 3 primeiras linhas referentes a metadados do Tesouro Nacional
    df_raw = pd.read_csv(io.StringIO(texto), sep=";", skiprows=3,
                         dtype=str, na_filter=False)

    print(f"  ✅ {len(df_raw)} municípios no Brasil")
    print(f"  Colunas: {list(df_raw.columns[:8])}...")

    # Extração dinâmica da data real da pesquisa no servidor
    linhas = texto.split("\n")
    data_pesquisa = linhas[0].strip().replace('"', '').replace('Data da Pesquisa: ', '')
    print(f"  Data da pesquisa no arquivo: {data_pesquisa}")

    # ── Filtragem Espacial (Paraíba) ──────────────────────────────────────────
    col_ibge = next((c for c in df_raw.columns if "ibge" in c.lower()), None)
    if not col_ibge:
        print(f"❌ Coluna IBGE não encontrada. Colunas: {list(df_raw.columns)}")
        return

    df_raw[col_ibge] = df_raw[col_ibge].astype(str)
    ibges_pb = set(municipios_df["cod_ibge"].tolist())
    df_pb = df_raw[df_raw[col_ibge].isin(ibges_pb)].copy()
    print(f"  Municípios PB encontrados: {len(df_pb)}")

    # ── Avaliação de Conformidade Fiscal ──────────────────────────────────────
    colunas_req = [c for c in df_raw.columns if c in REQUISITOS]
    registros = []
    
    for _, row in df_pb.iterrows():
        cod = row[col_ibge]
        nome_row = row.get("Nome do Ente Federado", "")

        # Mapeamento de irregularidades (sinalizadas por '!' ou campos vazios)
        pendencias = [REQUISITOS[c] for c in colunas_req
                      if row.get(c, "").strip() in ("!", "")]
        bloqueado = len(pendencias) > 0

        registros.append({
            "cod_ibge":        cod,
            "municipio":       nome_row,
            "bloqueado":       bloqueado,
            "qtd_pendencias":  len(pendencias),
            "pendencias":      " | ".join(pendencias) if pendencias else "REGULAR",
            "data_pesquisa":   data_pesquisa,
            "data_coleta":     HOJE,
            "fonte":           "CKAN-TesouroTransparente",
        })

    df_final = pd.DataFrame(registros)

    # ── Exportação de Dados ───────────────────────────────────────────────────
    snapshot = RAW_DIR / f"cauc_snapshot_{HOJE}.csv"
    df_final.to_csv(snapshot, index=False, encoding="utf-8-sig")
    df_final.to_csv(OUT_PROC, index=False, encoding="utf-8-sig")

    bloqueados = df_final["bloqueado"].sum()
    print(f"\n✅ Concluído!")
    print(f"   Total PB:         {len(df_final)}")
    print(f"   Com pendências:   {bloqueados}")
    print(f"   Regulares:        {len(df_final) - bloqueados}")
    print(f"   Salvo em:         {OUT_PROC.name}")
    print("=" * 70)

if __name__ == "__main__":
    main()