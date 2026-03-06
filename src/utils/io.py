import pandas as pd
from pathlib import Path


def ler_csv(caminho: Path, **kwargs) -> pd.DataFrame:
    return pd.read_csv(caminho, dtype={"cod_ibge": str}, encoding="utf-8-sig", **kwargs)


def salvar_csv(df: pd.DataFrame, caminho: Path, verbose: bool = True) -> None:
    df.to_csv(caminho, index=False, encoding="utf-8-sig")
    if verbose:
        print(f"  ✅ Salvo: {caminho.name} ({len(df)} linhas)")
