# SolveLicita

> **"Essa prefeitura vai me pagar?"** — a pergunta que nenhuma plataforma de licitações responde.

## O problema

25% das PMEs brasileiras estão inadimplentes. Um dos fatores de risco oculto
é fornecer para entes públicos que atrasam ou não pagam — mas não existe hoje
uma ferramenta pública que avalie a solvência do comprador público *antes* da
empresa investir tempo e recursos em participar da licitação.

## A solução

SolveLicita calcula um **Score de Solvência (0–100)** para municípios
brasileiros, cruzando fontes de dados públicos oficiais:

| Fonte | O que mede | Peso |
|-------|------------|------|
| SICONFI (Tesouro Nacional) | Execução orçamentária e restos a pagar | 45% |
| CAUC/STN | Bloqueios para recebimento de repasses federais | 20% |
| DataJud (CNJ) | Ações judiciais de fornecedores por inadimplência | 10% |
| PNCP | Histórico e volume de compras públicas | — |
| TCU | Achados críticos de auditoria | 7% |
| CEIS/CNEP | Sanções ativas | 3% |

## Status — Fase 0 (Paraíba, 223 municípios)

- [x] Tabela mestra de municípios PB (223 municípios, CNPJ, população)
- [x] Coleta SICONFI — RREO 2020–2025 (Anexo 01 + 07)
- [x] Processamento SICONFI — indicadores por município/ano (DuckDB)
- [x] Coleta CAUC — snapshot 24/02/2026 (220 com pendências)
- [x] Coleta PNCP — 54.139 licitações, 220 municípios, 2023–2026
- [ ] Coleta DataJud (TJPB + TRF5)
- [ ] Cálculo do score de solvência
- [ ] Mapa coroplético interativo (GeoPandas + Folium)
- [ ] Relatório narrativo público
- [ ] App Streamlit com busca por município

**Previsão de entrega pública: 15/04/2026**

## Como rodar localmente

```bash
git clone https://github.com/Fel-tby/solvelicita.git
cd solvelicita
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt

# Ordem de execução dos coletores
python src/collectors/municipios.py   # gera tabela mestra
python src/collectors/siconfi.py      # ~30 min, gera siconfi_rreo_pb.csv
python src/score/indicadores.py       # ~2 min, gera siconfi_indicadores_pb.csv
python src/collectors/cauc.py         # ~1 min, gera cauc_situacao_pb.csv
python src/collectors/pncp.py         # ~4h,   gera pncp_parcial.jsonl
python reconsolidar_pncp.py           # ~2 min, gera pncp_licitacoes_pb.csv
