# SolveLicita

> **"Essa prefeitura vai me pagar?"** — a pergunta que nenhuma plataforma de licitações responde.

## App ao vivo

**[https://solvelicita.streamlit.app](https://solvelicita.streamlit.app)**

---

## O problema

Fornecer para o poder público sem avaliar quem está do outro lado do contrato é um risco real e invisível. Não existe hoje uma ferramenta pública que avalie a solvência do comprador municipal *antes* da empresa assinar o contrato.

## A solução

SolveLicita calcula um **Score de Solvência (0–100)** para cada município brasileiro, cruzando fontes de dados públicos oficiais:

| Fonte | O que captura |
|---|---|
| SICONFI / RGF (Tesouro Nacional) | Liquidez, execução orçamentária, RP crônicos, transparência |
| CAUC / STN | Bloqueios para recebimento de repasses federais |
| FINBRA / DCA (STN) | Autonomia tributária |
| PNCP | Volume e padrão de compras públicas |

Metodologia completa em [`docs/METODOLOGIA.md`](docs/METODOLOGIA.md).

## Estrutura

    src/
    ├── collectors/   # coleta bruta por fonte (SICONFI, CAUC, PNCP, DCA...)
    ├── processors/   # limpeza e indicadores derivados
    ├── scorers/      # um scorer por indicador (lliq, eorcam, cauc, qsiconfi...)
    ├── engine/       # solvency.py — orquestrador e classifier
    └── utils/        # paths, io

    app/              # dashboard Streamlit (main.py + prep_data.py)
    data/             # raw/, processed/, outputs/
    docs/             # METODOLOGIA.md

## Status

**Fase 0 — Paraíba concluída**

- [x] Coleta SICONFI — 223 municípios, 2020–2025
- [x] Coleta CAUC — snapshot 24/02/2026
- [x] Coleta PNCP — 54.139 licitações, 220 municípios, 2023–2026
- [x] Coleta FINBRA/DCA — autonomia tributária, 2020–2025
- [x] Score de solvência — 6 indicadores, 100 pts, decay por defasagem
- [x] Mapa coroplético interativo — Streamlit + Folium
- [ ] Relatório narrativo público — Paraíba
- [ ] Expansão para demais estados

## Como rodar localmente

    git clone https://github.com/Fel-tby/solvelicita.git
    cd solvelicita
    python -m venv venv
    venv\Scripts\activate          # Windows
    # source venv/bin/activate     # Linux/macOS
    pip install -r requirements.txt

    # 1. Processar indicadores SICONFI
    python src/processors/siconfi_processor.py

    # 2. Calcular score de solvência
    python src/engine/solvency.py

    # 3. Enriquecer com dados PNCP
    python src/processors/pncp_agregador.py

    # 4. Preparar GeoJSON do app (apenas uma vez, ou após novo score)
    python app/prep_data.py

    # 5. Rodar o dashboard
    streamlit run app/main.py

## Classificação de risco

| Score | Classificação |
|---|---|
| ≥ 75 | 🟢 Risco Baixo |
| 55 – 74 | 🟡 Risco Médio |
| 35 – 54 | 🔴 Risco Alto |
| < 35 | ⛔ Crítico |

Scores podem ser rebaixados por caps de transparência (municípios que não entregam RREO) ou cronicidade de dívidas (RP Processados acima de 3% por múltiplos anos).

---

Dados 100% públicos · Código aberto · [Metodologia](docs/METODOLOGIA.md) · [MIT License](LICENSE)
