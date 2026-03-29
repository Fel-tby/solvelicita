from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent

# Fábrica de paths por UF
def get_paths(uf: str) -> dict:
    """
    Retorna dict com todos os paths relevantes para uma UF específica.
    Garante que os diretórios existam ao ser chamada.
    """
    uf = uf.upper()
    paths = {
        "raw_siconfi" : ROOT / "data" / "raw"       / "siconfi" / uf,
        "raw_cauc"    : ROOT / "data" / "raw"       / "cauc"    / uf,
        "raw_dca"     : ROOT / "data" / "raw"       / "dca"     / uf,
        "raw_pncp"    : ROOT / "data" / "raw"       / "pncp"    / uf,
        "processed"   : ROOT / "data" / "processed" / uf,
        "outputs"     : ROOT / "data" / "outputs"   / uf,
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    return paths


# Aliases globais de compatibilidade (apontam para PB)
# Mantêm todos os módulos ainda não migrados funcionando sem alteração.
# Removidos no Bloco 9, após todos os módulos usarem get_paths(uf).
RAW       = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed" / "PB"
OUTPUTS   = ROOT / "data" / "outputs"   / "PB"
APP_DATA  = ROOT / "app"  / "data"

OUTPUTS.mkdir(parents=True, exist_ok=True)
PROCESSED.mkdir(parents=True, exist_ok=True)