"""
pipeline.py — Orquestrador central do SolveLicita.
Localização: raiz do projeto (ao lado de README.md).

Executa o pipeline completo ou etapas individuais, em modo full ou incremental.

Uso:
    python pipeline.py                          # interativo
    python pipeline.py --mode full              # força full, todas as etapas
    python pipeline.py --mode incremental       # força incremental, todas as etapas
    python pipeline.py --steps process,score    # pula coleta
    python pipeline.py --steps app              # só regera o GeoJSON
    python pipeline.py --mode incremental --steps collect,process,score,app

Etapas disponíveis:
    collect  — coleta bruta (municipios, cauc, siconfi, dca, pncp)
    process  — processamento analítico (todos os processors)
    score    — cálculo do score (solvency.py + pncp_agregador.py)
    app      — gera GeoJSON para o dashboard (prep_data.py)
"""

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

# ── Imports dos módulos ───────────────────────────────────────────────────────
from src.collectors import municipios, cauc, siconfi, dca, pncp
from src.processors import cauc_processor, siconfi_processor, dca_processor
from src.processors import pncp_processor, pncp_agregador
from src.engine     import solvency

# prep_data fica em app/ — import dinâmico para não exigir geopandas no path raiz
import importlib.util as _ilu

def _importar_prep_data():
    spec = _ilu.spec_from_file_location("prep_data", ROOT / "app" / "prep_data.py")
    mod  = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

# ── Constantes ────────────────────────────────────────────────────────────────
ETAPAS_VALIDAS = {"collect", "process", "score", "app"}


# ── UI de seleção de modo ─────────────────────────────────────────────────────

def selecionar_modo() -> str:
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║            SolveLicita — Pipeline de Dados           ║")
    print("╠══════════════════════════════════════════════════════╣")
    print("║                                                      ║")
    print("║  [1] Full         — histórico completo               ║")
    print("║                     (use na primeira execução)       ║")
    print("║                                                      ║")
    print("║  [2] Incremental  — apenas período recente           ║")
    print("║                     (CAUC: snapshot atual            ║")
    print("║                      SICONFI: ano anterior + corrente║")
    print("║                      DCA: último exercício           ║")
    print("║                      PNCP: últimos 60 dias)          ║")
    print("║                                                      ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    while True:
        escolha = input("  Modo de coleta [1/2]: ").strip()
        if escolha == "1":
            return "full"
        if escolha == "2":
            return "incremental"
        print("  Opção inválida. Digite 1 para Full ou 2 para Incremental.")


def selecionar_etapas() -> set[str]:
    print()
    print("  Etapas a executar:")
    print("    [1] Todas                    (collect → process → score → app)")
    print("    [2] process + score + app    (dados já coletados)")
    print("    [3] score + app              (dados já processados)")
    print("    [4] Apenas app               (só regera o GeoJSON)")
    print("    [5] Personalizado            (digitar etapas)")
    print()

    while True:
        escolha = input("  Etapas [1/2/3/4/5]: ").strip()

        if escolha == "1":
            return {"collect", "process", "score", "app"}
        if escolha == "2":
            return {"process", "score", "app"}
        if escolha == "3":
            return {"score", "app"}
        if escolha == "4":
            return {"app"}
        if escolha == "5":
            raw    = input("  Digite etapas (collect,process,score,app): ").strip()
            etapas = {e.strip() for e in raw.split(",")}
            invalidas = etapas - ETAPAS_VALIDAS
            if invalidas:
                print(f"  Etapas inválidas: {invalidas}. Use: collect, process, score, app.")
                continue
            return etapas

        print("  Opção inválida.")


# ── Etapas do pipeline ────────────────────────────────────────────────────────

def etapa_collect(mode: str) -> None:
    print("\n" + "═" * 55)
    print(f"  ETAPA: COLETA  [{mode.upper()}]")
    print("═" * 55)

    print("\n[1/5] Municípios PB...")
    municipios.run()

    print("\n[2/5] CAUC...")
    cauc.run()

    print(f"\n[3/5] SICONFI [{mode}]...")
    siconfi.run(mode=mode)

    print(f"\n[4/5] DCA [{mode}]...")
    dca.run(mode=mode)

    print(f"\n[5/5] PNCP [{mode}]...")
    pncp.run(mode=mode)


def etapa_process() -> None:
    print("\n" + "═" * 55)
    print("  ETAPA: PROCESSAMENTO")
    print("═" * 55)

    print("\n[1/4] CAUC processor...")
    cauc_processor.run()

    print("\n[2/4] SICONFI processor...")
    siconfi_processor.run()

    print("\n[3/4] DCA processor...")
    dca_processor.run()

    print("\n[4/4] PNCP processor...")
    pncp_processor.run()


def etapa_score() -> None:
    print("\n" + "═" * 55)
    print("  ETAPA: SCORE")
    print("═" * 55)

    print("\n[1/2] Solvency engine...")
    solvency.run()

    print("\n[2/2] PNCP agregador...")
    pncp_agregador.run()


def etapa_app() -> None:
    print("\n" + "═" * 55)
    print("  ETAPA: APP — GeoJSON")
    print("═" * 55)

    print("\n[1/1] Gerando pb_score.geojson...")
    prep_data = _importar_prep_data()
    prep_data.run()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = sys.argv[1:]

    # ── Resolve modo ─────────────────────────────────────────────────────────
    if "--mode" in args:
        idx  = args.index("--mode")
        mode = args[idx + 1] if idx + 1 < len(args) else None
        if mode not in ("full", "incremental"):
            print(f"  Erro: --mode deve ser 'full' ou 'incremental'. Recebido: '{mode}'")
            sys.exit(1)
    else:
        mode = None

    # ── Resolve etapas ────────────────────────────────────────────────────────
    if "--steps" in args:
        idx    = args.index("--steps")
        raw    = args[idx + 1] if idx + 1 < len(args) else ""
        etapas = {e.strip() for e in raw.split(",")}
        invalidas = etapas - ETAPAS_VALIDAS
        if invalidas:
            print(f"  Erro: etapas inválidas: {invalidas}. Use: collect, process, score, app.")
            sys.exit(1)
    else:
        etapas = None

    # ── Interatividade ────────────────────────────────────────────────────────
    if mode is None:
        if etapas is None or "collect" in etapas:
            mode = selecionar_modo()
        else:
            mode = "incremental"   # neutro — não usado em process/score/app

    if etapas is None:
        etapas = selecionar_etapas()

    # ── Sumário ───────────────────────────────────────────────────────────────
    etapas_str = " → ".join(
        e for e in ["collect", "process", "score", "app"] if e in etapas
    )
    print()
    print("  ┌─────────────────────────────────────────┐")
    print(f"  │  Modo   : {mode:<31}│")
    print(f"  │  Etapas : {etapas_str:<31}│")
    print("  └─────────────────────────────────────────┘")
    print()
    input("  Pressione Enter para iniciar ou Ctrl+C para cancelar...")

    t0 = time.time()

    try:
        if "collect" in etapas:
            etapa_collect(mode)

        if "process" in etapas:
            etapa_process()

        if "score" in etapas:
            etapa_score()

        if "app" in etapas:
            etapa_app()

    except KeyboardInterrupt:
        print("\n\n  Pipeline interrompido pelo usuário.")
        sys.exit(0)

    elapsed = time.time() - t0
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print(f"║  ✅ Pipeline concluído em {elapsed/60:.1f} min"
          + " " * (27 - len(f"{elapsed/60:.1f}")) + "║")
    print("║  Próximo passo: streamlit run app/main.py            ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()


if __name__ == "__main__":
    main()