"""
bigquery_loader.py — Interface única de leitura/escrita no BigQuery.

"""

import json
import os
import re
import logging
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent

_CSV_FALLBACK: dict[str, str] = {
    "mart_indicadores_municipios" : "data/processed/PB/siconfi_indicadores_pb.csv",
    "mart_pncp_municipios"        : "data/outputs/PB/score_municipios_pb_pncp.csv",
    "score_municipios"            : "data/outputs/PB/score_municipios_pb.csv",
}

_client       = None
_BQ_AVAILABLE = None


def _check_bq_available() -> bool:
    global _BQ_AVAILABLE
    if _BQ_AVAILABLE is not None:
        return _BQ_AVAILABLE
    try:
        import google.cloud.bigquery  # noqa: F401
        _BQ_AVAILABLE = True
    except ImportError:
        _BQ_AVAILABLE = False
    return _BQ_AVAILABLE


def _cfg() -> dict:
    return {
        "project" : os.getenv("GCP_PROJECT_ID", "solvelicita"),
        "sa_path" : os.getenv("GCP_SA_KEY_PATH", ""),
        "dataset" : os.getenv("BQ_DATASET", "raw"),
        "enabled" : os.getenv("BQ_ENABLED", "false").lower() == "true",
    }


def _get_client():
    global _client
    if _client is not None:
        return _client
    if not _check_bq_available():
        raise RuntimeError(
            "google-cloud-bigquery não instalado. "
            "Execute: pip install google-cloud-bigquery"
        )
    from google.cloud import bigquery
    from google.oauth2 import service_account

    cfg     = _cfg()
    sa_path = cfg["sa_path"]
    if sa_path and Path(sa_path).exists():
        creds   = service_account.Credentials.from_service_account_file(sa_path)
        _client = bigquery.Client(project=cfg["project"], credentials=creds)
    else:
        _client = bigquery.Client(project=cfg["project"])
    return _client


def _sanitize(df: pd.DataFrame, uf: str) -> pd.DataFrame:
    """
    Prepara o DataFrame para upload no BigQuery:
    - Remove colunas duplicadas
    - Sanitiza nomes de colunas (só letras, números e _)
    - Garante que nomes não começam com número
    - Adiciona coluna uf se não existir
    - Serializa dicts/lists para JSON válido antes de converter para STRING
    - Converte tudo para STRING (tipagem na camada raw — casting feito no dbt)
    - Substitui "nan" / "None" por None (NULL no BQ)
    """
    # Remove duplicatas
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # Sanitiza nomes
    new_cols = []
    seen     = {}
    for i, col in enumerate(df.columns):
        name = re.sub(r"[^a-zA-Z0-9_]", "_", str(col)).lower()
        # Colunas que começam com dígito ganham prefixo "c_" em vez de serem
        # truncadas — preserva semântica (ex: "1_1" → "c_1_1").
        if name and name[0].isdigit():
            name = "c_" + name
        name = re.sub(r"^_+", "", name) or f"col_{i}"
        # Resolve colisões após sanitização
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        new_cols.append(name)
    df.columns = new_cols

    # Garante coluna uf
    if "uf" not in df.columns:
        df["uf"] = uf.upper()

    # Serializa dicts e lists para JSON válido antes do astype(str).
    # Sem isso, str({"cnpj": "..."}) produz repr Python com aspas simples,
    # que o BigQuery não consegue parsear com JSON_VALUE().
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x, ensure_ascii=False)
            if isinstance(x, (dict, list))
            else x
        )

    # Tudo como STRING — raw é zona de aterrissagem, não de tipos
    df = df.astype(str)
    df = df.replace({"nan": None, "None": None, "NaT": None, "<NA>": None})

    return df


def upload_raw(
    df: pd.DataFrame,
    table: str,
    uf: str,
    write_mode: str = "append",
) -> None:
    """
    Faz upload de um DataFrame para a camada raw do BigQuery.

    Parâmetros
    ----------
    df         : DataFrame a ser enviado
    table      : nome da tabela destino (ex: "siconfi_rreo", "cauc", "dca")
    uf         : sigla do estado (ex: "PB", "CE")
    write_mode : "append" (padrão) | "replace"
    """
    cfg = _cfg()

    if not cfg["enabled"] or not _check_bq_available() or not cfg["sa_path"]:
        logger.info(
            "[BQ stub] upload_raw ignorado: table=%s.%s uf=%s linhas=%d",
            cfg["dataset"], table, uf.upper(), len(df),
        )
        return

    from google.cloud import bigquery

    client     = _get_client()
    table_ref  = f"{cfg['project']}.{cfg['dataset']}.{table}"
    bq_mode    = (
        bigquery.WriteDisposition.WRITE_APPEND
        if write_mode == "append"
        else bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    df_upload  = _sanitize(df, uf)

    schema = [
        bigquery.SchemaField(col, "STRING")
        for col in df_upload.columns
    ]
    job_config = bigquery.LoadJobConfig(
        write_disposition=bq_mode,
        schema=schema,
    )

    job = client.load_table_from_dataframe(df_upload, table_ref, job_config=job_config)
    job.result()
    logger.info("[BQ] ✅ upload_raw: %s (%d linhas)", table_ref, len(df_upload))
    print(f"  [BQ] ✅ {table_ref}: {len(df_upload):,} linhas enviadas")


def read_mart(table: str, uf: str | None = None) -> pd.DataFrame:
    """
    Lê uma tabela do mart do BigQuery.
    Fallback para CSV local quando BigQuery não está disponível.
    """
    cfg = _cfg()

    if not cfg["enabled"] or not _check_bq_available() or not cfg["sa_path"]:
        return _read_mart_csv_fallback(table, uf)

    client    = _get_client()
    table_ref = f"{cfg['project']}.mart.{table}"
    uf_filter = f"WHERE uf = '{uf.upper()}'" if uf else ""
    query     = f"SELECT * FROM `{table_ref}` {uf_filter}"

    logger.info("[BQ] read_mart: %s uf=%s", table_ref, uf or "nacional")
    return client.query(query).to_dataframe()


def read_intermediate(table: str, uf: str | None = None) -> pd.DataFrame:
    """
    Lê uma tabela da camada intermediate do BigQuery.
    Retorna DataFrame vazio quando BigQuery não está disponível.
    """
    cfg = _cfg()

    if not cfg["enabled"] or not _check_bq_available() or not cfg["sa_path"]:
        logger.warning(
            "[BQ stub] read_intermediate ignorado — sem fallback CSV para '%s'. "
            "Retornando DataFrame vazio.",
            table,
        )
        return pd.DataFrame()

    client    = _get_client()
    table_ref = f"{cfg['project']}.intermediate.{table}"
    uf_filter = f"WHERE uf = '{uf.upper()}'" if uf else ""
    query     = f"SELECT * FROM `{table_ref}` {uf_filter}"

    logger.info("[BQ] read_intermediate: %s uf=%s", table_ref, uf or "nacional")
    return client.query(query).to_dataframe()


def _read_mart_csv_fallback(table: str, uf: str | None) -> pd.DataFrame:
    rel_path = _CSV_FALLBACK.get(table)
    if not rel_path:
        logger.warning(
            "[BQ stub] Sem fallback CSV para tabela '%s'. Retornando DataFrame vazio.",
            table,
        )
        return pd.DataFrame()

    if uf and uf.upper() != "PB":
        rel_path = rel_path.replace("/PB/", f"/{uf.upper()}/")

    full_path = _ROOT / rel_path
    if not full_path.exists():
        logger.warning(
            "[BQ stub] Fallback CSV não encontrado: %s. Retornando DataFrame vazio.",
            full_path,
        )
        return pd.DataFrame()

    logger.info("[BQ stub] read_mart via CSV local: %s", full_path.name)
    return pd.read_csv(full_path, dtype={"cod_ibge": str})
