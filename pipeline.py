"""
pipeline.py — Orquestrador central do SolveLicita.
Localização: raiz do projeto (ao lado de README.md).

Executa o pipeline completo ou etapas individuais, em modo full ou incremental.

Uso:
    python pipeline.py                                            # interativo
    python pipeline.py --uf CE                                   # outro estado, interativo
    python pipeline.py --mode full                               # força full, todas as etapas
    python pipeline.py --mode incremental                        # força incremental, todas as etapas
    python pipeline.py --steps process,score                     # pula coleta
    python pipeline.py --steps app                               # só regera o GeoJSON
    python pipeline.py --steps score,sync                        # score + envia pro Supabase
    python pipeline.py --uf CE --mode incremental --steps collect,score,sync

Etapas disponíveis:
    collect — coleta bruta (municipios, cauc, siconfi, dca, pncp)
    process — processamento analítico (todos os processors)
    score   — cálculo do score (solvency.py + pncp_agregador.py)
    app     — gera GeoJSON para o dashboard (prep_data.py)
    sync    — sincroniza dados com o Supabase
"""

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from src.collectors import municipios, cauc, siconfi, dca, pncp
from src.processors import cauc_processor, siconfi_processor, dca_processor
from src.processors import pncp_processor, pncp_agregador
from src.engine import solvency
from src.utils.paths import get_paths
from src.utils.supabase_sync import run as supabase_sync

import importlib.util as _ilu

def _importar_prep_data():
    spec = _ilu.spec_from_file_location("prep_data", ROOT / "app" / "prep_data.py")
    mod  = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ETAPAS_VALIDAS = {"collect", "process", "score", "app", "sync"}
ETAPAS_ORDEM   = ["collect", "process", "score", "app", "sync"]


# UI de seleção

def selecionar_uf() -> str:
    print()
    print("  UF alvo (ex: PB, CE, RN). Enter = PB")
    uf = input("  UF: ").strip().upper()
    return uf if uf else "PB"


def selecionar_modo() -> str:
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║         SolveLicita — Pipeline de Dados             ║")
    print("╠══════════════════════════════════════════════════════╣")
    print("║                                                      ║")
    print("║  [1] Full — histórico completo                      ║")
    print("║      (use na primeira execução)                      ║")
    print("║                                                      ║")
    print("║  [2] Incremental — apenas período recente           ║")
    print("║      (CAUC: snapshot atual                          ║")
    print("║       SICONFI: ano anterior + corrente              ║")
    print("║       DCA: último exercício                         ║")
    print("║       PNCP: últimos 60 dias)                        ║")
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
    print("  [1] Todas (collect → process → score → app → sync)")
    print("  [2] Todas sem sync (collect → process → score → app)")
    print("  [3] process + score + app (dados já coletados)")
    print("  [4] score + app (dados já processados)")
    print("  [5] Apenas app (só regera o GeoJSON)")
    print("  [6] Apenas sync (só envia para o Supabase)")
    print("  [7] Personalizado (digitar etapas)")
    print()
    while True:
        escolha = input("  Etapas [1/2/3/4/5/6/7]: ").strip()
        if escolha == "1": return {"collect", "process", "score", "app", "sync"}
        if escolha == "2": return {"collect", "process", "score", "app"}
        if escolha == "3": return {"process", "score", "app"}
        if escolha == "4": return {"score", "app"}
        if escolha == "5": return {"app"}
        if escolha == "6": return {"sync"}
        if escolha == "7":
            raw      = input("  Digite etapas (collect,process,score,app,sync): ").strip()
            etapas   = {e.strip() for e in raw.split(",")}
            invalidas = etapas - ETAPAS_VALIDAS
            if invalidas:
                print(f"  Etapas inválidas: {invalidas}. Use: collect, process, score, app, sync.")
                continue
            return etapas
        print("  Opção inválida.")


# Etapas do pipeline

def etapa_collect(mode: str, uf: str) -> None:
    print("\n" + "═" * 55)
    print(f"  ETAPA: COLETA [{mode.upper()}] — {uf}")
    print("═" * 55)

    print("\n[1/5] Municípios...")
    municipios.run(uf=uf)

    print("\n[2/5] CAUC...")
    cauc.run(uf=uf)

    print(f"\n[3/5] SICONFI [{mode}]...")
    siconfi.run(mode=mode, uf=uf)

    print(f"\n[4/5] DCA [{mode}]...")
    dca.run(mode=mode, uf=uf)

    print(f"\n[5/5] PNCP [{mode}]...")
    pncp.run(mode=mode, uf=uf)


def etapa_process(uf: str) -> None:
    """
    Processors legados — ainda sem parâmetro uf.
    Receberão uf no Bloco 8, quando forem substituídos pelos
    postprocessors + dbt. O parâmetro uf é recebido aqui para
    manter a assinatura consistente desde já.
    """
    print("\n" + "═" * 55)
    print(f"  ETAPA: PROCESSAMENTO — {uf}")
    print("═" * 55)

    print("\n[1/4] CAUC processor...")
    cauc_processor.run()

    print("\n[2/4] SICONFI processor...")
    siconfi_processor.run()

    print("\n[3/4] DCA processor...")
    dca_processor.run()

    print("\n[4/4] PNCP processor...")
    pncp_processor.run()


def etapa_score(uf: str) -> None:
    """
    Engine legado — ainda sem parâmetro uf.
    Receberá uf no Bloco 8, junto com a migração para leitura do BigQuery.
    """
    print("\n" + "═" * 55)
    print(f"  ETAPA: SCORE — {uf}")
    print("═" * 55)

    print("\n[1/2] Solvency engine...")
    solvency.run()

    print("\n[2/2] PNCP agregador...")
    pncp_agregador.run()


def etapa_app(uf: str) -> None:
    print("\n" + "═" * 55)
    print(f"  ETAPA: APP — GeoJSON — {uf}")
    print("═" * 55)

    print("\n[1/1] Gerando pb_score.geojson...")
    prep_data = _importar_prep_data()
    prep_data.run()


def etapa_sync(uf: str) -> None:
    """
    supabase_sync ainda sem parâmetro uf.
    Receberá uf no Bloco 8, junto com a migração da tabela Supabase.
    """
    print("\n" + "═" * 55)
    print(f"  ETAPA: SYNC — Supabase — {uf}")
    print("═" * 55)

    print("\n[1/1] Sincronizando com Supabase...")
    supabase_sync()


# Main ─────────────────────────────────────────────────────────────────────────

def main() -> None:
    args = sys.argv[1:]

    # Resolve uf
    if "--uf" in args:
        idx = args.index("--uf")
        uf  = args[idx + 1].upper() if idx + 1 < len(args) else "PB"
    else:
        uf_inline = next((a.split("=", 1)[1] for a in args if a.startswith("--uf=")), None)
        uf        = uf_inline.upper() if uf_inline else None

    # Resolve mode
    if "--mode" in args:
        idx  = args.index("--mode")
        mode = args[idx + 1] if idx + 1 < len(args) else None
        if mode not in ("full", "incremental"):
            print(f"  Erro: --mode deve ser 'full' ou 'incremental'. Recebido: '{mode}'")
            sys.exit(1)
    else:
        mode_inline = next((a.split("=", 1)[1] for a in args if a.startswith("--mode=")), None)
        mode        = mode_inline if mode_inline in ("full", "incremental") else None

    # Resolve etapas
    if "--steps" in args:
        idx    = args.index("--steps")
        raw    = args[idx + 1] if idx + 1 < len(args) else ""
        etapas = {e.strip() for e in raw.split(",")}
        invalidas = etapas - ETAPAS_VALIDAS
        if invalidas:
            print(f"  Erro: etapas inválidas: {invalidas}. Use: collect, process, score, app, sync.")
            sys.exit(1)
    else:
        steps_inline = next((a.split("=", 1)[1] for a in args if a.startswith("--steps=")), None)
        if steps_inline:
            etapas    = {e.strip() for e in steps_inline.split(",")}
            invalidas = etapas - ETAPAS_VALIDAS
            if invalidas:
                print(f"  Erro: etapas inválidas: {invalidas}.")
                sys.exit(1)
        else:
            etapas = None

    # Interatividade — só pergunta o que não veio via CLI
    if uf is None:
        uf = selecionar_uf()

    if mode is None:
        if etapas is None or "collect" in etapas:
            mode = selecionar_modo()
        else:
            mode = "incremental"  # neutro — não usado em process/score/app/sync

    if etapas is None:
        etapas = selecionar_etapas()

    # Garante diretórios da UF antes de qualquer etapa
    get_paths(uf)

    # Sumário
    etapas_str = " → ".join(e for e in ETAPAS_ORDEM if e in etapas)

    print()
    print("  ┌─────────────────────────────────────────────┐")
    print(f"  │  UF    : {uf:<33}│")
    print(f"  │  Modo  : {mode:<33}│")
    print(f"  │  Etapas: {etapas_str:<33}│")
    print("  └─────────────────────────────────────────────┘")
    print()
    input("  Pressione Enter para iniciar ou Ctrl+C para cancelar...")

    t0 = time.time()

    try:
        if "collect" in etapas:
            etapa_collect(mode, uf)

        if "process" in etapas:
            etapa_process(uf)

        if "score" in etapas:
            etapa_score(uf)

        if "app" in etapas:
            etapa_app(uf)

        if "sync" in etapas:
            etapa_sync(uf)

    except KeyboardInterrupt:
        print("\n\n  Pipeline interrompido pelo usuário.")
        sys.exit(0)

    elapsed = time.time() - t0
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print(f"║  ✅ Pipeline concluído em {elapsed/60:.1f} min"
          + " " * (27 - len(f"{elapsed/60:.1f}")) + "║")
    print("║  Dashboard: https://solvelicita.tech                ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()


if __name__ == "__main__":
    main()