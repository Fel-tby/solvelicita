from pathlib import Path

ROOT      = Path(__file__).resolve().parent.parent.parent
RAW       = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
OUTPUTS   = ROOT / "data" / "outputs"
APP_DATA  = ROOT / "app" / "data"

OUTPUTS.mkdir(parents=True, exist_ok=True)
PROCESSED.mkdir(parents=True, exist_ok=True)
