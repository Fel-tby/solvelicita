# SolveLicita

> **"Essa prefeitura vai me pagar?"** — a pergunta que nenhuma plataforma de licitações responde.

## O problema

25% das PMEs brasileiras estão inadimplentes. Um dos fatores de risco oculto é fornecer para entes públicos que atrasam ou não pagam — mas não existe hoje uma ferramenta pública que avalie a solvência do comprador público *antes* da empresa investir tempo e recursos em participar da licitação.

## A solução

SolveLicita calcula um **Score de Solvência (0–100)** para municípios brasileiros, cruzando fontes de dados públicos oficiais:

| Fonte | O que mede |
|-------|------------|
| SICONFI (Tesouro Nacional) | Execução orçamentária e restos a pagar |
| CAUC/STN | Bloqueios para recebimento de repasses federais |
| DataJud (CNJ) | Ações judiciais de fornecedores por inadimplência |
| PNCP | Histórico de compras públicas |
| TCU | Achados críticos de auditoria |

## Status atual

**Fase 0 em desenvolvimento** — Relatório público dos 223 municípios da Paraíba

- [x] Tabela mestra de municípios PB (223 municípios)
- [x] Coleta SICONFI (223 municípios PB, 2020–2025)
- [x] Coleta CAUC (snapshot 24/02/2026)
- [x] Coleta PNCP (54.139 licitações, 220 municípios, 2023–2026)
- [ ] Coleta DataJud (TJPB + TRF5)
- [ ] Cálculo do score de solvência
- [ ] Mapa coroplético interativo
- [ ] Relatório narrativo público
- [ ] App Streamlit com busca por município

## Como rodar localmente

```bash
git clone https://github.com/Fel-tby/solvelicita.git
cd solvelicita
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
