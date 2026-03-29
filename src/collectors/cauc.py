"""
Coletor CAUC — CKAN Tesouro Transparente.
Responsabilidade: baixar o relatório nacional, filtrar para municípios da
UF informada e salvar os dados BRUTOS (sem classificação de pendências).

A classificação de pendências por gravidade é feita por:
    src/processors/cauc_processor.py

Rodar individualmente:
    python src/collectors/cauc.py
    python src/collectors/cauc.py --uf CE
"""

import sys
import io
import pandas as pd
import requests
import urllib3
from pathlib import Path
from datetime import date

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.paths import get_paths, RAW
from utils.bigquery_loader import upload_raw

URL_CAUC_BULK = (
    "https://www.tesourotransparente.gov.br/ckan/dataset/"
    "72b5f371-0c35-4613-8076-c99c821a6410/resource/"
    "07af297a-5e59-494a-a88a-55ddfd2f4b01/download/"
    "relatorio-situacao-de-varios-entes---municipios---uf-todas---abrangencia-1.csv"
)

# Mapeamento dos códigos de requisito CAUC para nomes semânticos no BigQuery.
# O CSV em disco mantém os códigos originais ("1.1", "1.2" etc.) para que o
# cauc_processor.py legado continue funcionando sem alteração.
# O rename é aplicado apenas no DataFrame enviado ao BQ.
CAUC_COL_MAP = {
    # Metadados
    "UF":                    "uf",
    "Nome do Ente Federado": "municipio",
    "Código IBGE":           "cod_ibge",
    "Código SIAFI":          "cod_siafi",
    "Região":                "regiao",
    "População":             "populacao",
    "Fonte":                 "fonte",
    # Requisitos
    "1.1":   "req_previdenciaria_rpps",
    "1.2":   "req_fiscal_rfb",
    "1.3":   "req_pgfn",
    "1.4":   "req_fgts",
    "1.5":   "req_trabalhista_tst",
    "2.1.1": "req_lrf_pessoal_exec",
    "2.1.2": "req_lrf_pessoal_leg",
    "3.1.1": "req_siops",
    "3.1.2": "req_siops_demo",
    "3.2.1": "req_siope",
    "3.2.2": "req_siope_demo",
    "3.2.3": "req_siope_compl",
    "3.2.4": "req_siope_obs",
    "3.3":   "req_siga",
    "3.4.1": "req_siconv_pc",
    "3.4.2": "req_siconv_debitos",
    "3.5":   "req_cadin",
    "3.6":   "req_tcu",
    "3.7":   "req_cgu",
    "4.1":   "req_sistn_divida",
    "4.2":   "req_sistn_garantias",
    "5.1":   "req_siconfi_rreo",
    "5.2":   "req_siconfi_rgf",
    "5.3":   "req_siconfi_balanco",
    "5.4":   "req_siconfi_dca",
    "5.5":   "req_siconfi_pcasp",
    "5.6":   "req_siconfi_dcasp",
    "5.7":   "req_siconfi_mcasp",
}


def run(uf: str = "PB") -> pd.DataFrame:
    """
    Baixa o CSV nacional do CAUC, filtra municípios da UF e salva raw.

    Outputs:
        raw/cauc/<UF>/cauc_raw_<uf>_<HOJE>.csv  — snapshot datado
        raw/cauc/<UF>/cauc_raw_<uf>.csv          — latest (sobrescrito a cada coleta)
        raw/cauc/cauc_raw_<uf>.csv               — legacy flat (compat processors)

    Retorna o DataFrame bruto filtrado (com colunas originais do CKAN).
    """
    uf    = uf.upper()
    hoje  = date.today().strftime("%Y-%m-%d")
    paths = get_paths(uf)
    raw_cauc_uf = paths["raw_cauc"]

    print("=" * 70)
    print(f"  Coletor CAUC — CKAN Tesouro Transparente")
    print(f"  UF: {uf} | Execução: {hoje}")
    print("=" * 70)

    tabela_mun = raw_cauc_uf.parent.parent.parent / "processed" / uf / f"municipios_{uf.lower()}_tabela.csv"
    if not tabela_mun.exists():
        raise FileNotFoundError(
            f"Tabela de municípios não encontrada: {tabela_mun}\n"
            f"Execute primeiro: python src/collectors/municipios.py --uf {uf}"
        )

    municipios_df = pd.read_csv(tabela_mun, dtype={"cod_ibge": str})
    ibges_uf      = set(municipios_df["cod_ibge"].tolist())

    print(f"\n  Baixando CSV nacional do CKAN...")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    resp    = requests.get(URL_CAUC_BULK, headers=headers, verify=False, timeout=60)
    resp.raise_for_status()

    try:
        texto = resp.content.decode("utf-8-sig")
    except UnicodeDecodeError:
        texto = resp.content.decode("iso-8859-1")

    df_raw = pd.read_csv(
        io.StringIO(texto), sep=";", skiprows=3, dtype=str, na_filter=False
    )
    print(f"  ✅ {len(df_raw)} municípios no Brasil")

    linhas        = texto.split("\n")
    data_pesquisa = linhas[0].strip().replace('"', '').replace("Data da Pesquisa: ", "")
    print(f"  Data da pesquisa: {data_pesquisa}")

    col_ibge = next((c for c in df_raw.columns if "ibge" in c.lower()), None)
    if not col_ibge:
        raise ValueError(f"Coluna IBGE não encontrada. Colunas: {list(df_raw.columns)}")

    df_raw[col_ibge] = df_raw[col_ibge].astype(str)
    df_uf            = df_raw[df_raw[col_ibge].isin(ibges_uf)].copy()
    df_uf["data_pesquisa"] = data_pesquisa
    df_uf["data_coleta"]   = hoje

    print(f"  Municípios {uf} encontrados: {len(df_uf)}")

    # Primário — nova estrutura UF subfolder
    # CSV salvo com colunas originais do CKAN ("1.1", "1.2" etc.)
    # para que o cauc_processor.py legado continue funcionando sem alteração.
    df_uf.to_csv(raw_cauc_uf / f"cauc_raw_{uf.lower()}_{hoje}.csv",
                 index=False, encoding="utf-8-sig")
    df_uf.to_csv(raw_cauc_uf / f"cauc_raw_{uf.lower()}.csv",
                 index=False, encoding="utf-8-sig")
    print(f"  Salvo em: raw/cauc/{uf}/cauc_raw_{uf.lower()}.csv")

    # Legacy flat — processors leem daqui até o Bloco 9
    raw_cauc_flat = RAW / "cauc"
    raw_cauc_flat.mkdir(parents=True, exist_ok=True)
    df_uf.to_csv(raw_cauc_flat / f"cauc_raw_{uf.lower()}.csv",
                 index=False, encoding="utf-8-sig")
    print(f"  [compat] raw/cauc/cauc_raw_{uf.lower()}.csv: escrito para processors legados")

    print("=" * 70)

    # Upload ao BigQuery com nomes semânticos.
    # df_bq é uma cópia local — não altera df_uf nem os CSVs em disco.
    df_bq = df_uf.rename(columns=CAUC_COL_MAP)
    upload_raw(df_bq, "cauc", uf)

    return df_uf   # retorna com colunas originais do CKAN


if __name__ == "__main__":
    args   = sys.argv[1:]
    uf_arg = "PB"
    for i, arg in enumerate(args):
        if arg == "--uf" and i + 1 < len(args):
            uf_arg = args[i + 1]
        elif arg.startswith("--uf="):
            uf_arg = arg.split("=", 1)[1]
    run(uf=uf_arg)
