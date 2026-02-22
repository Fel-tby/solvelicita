# SolveLicita

> **"Essa prefeitura vai me pagar?"** — a pergunta que nenhuma plataforma de licitações responde.

## O problema

25% das PMEs brasileiras estão inadimplentes. Um dos fatores de risco oculto é fornecer para entes públicos que atrasam ou não pagam — mas não existe hoje uma ferramenta pública que avalie a solvência do comprador público *antes* da empresa investir tempo e recursos em participar da licitação.

## A solução

SolveLicita calcula um **Score de Solvência (0–100)** para municípios brasileiros, cruzando fontes de dados públicos oficiais:

| Fonte | O que mede |
|-------|------------|
| SICONFI (Tesouro Nacional) | Execução orçamentária e restos a pagar |
| PNCP | Histórico de compras públicas |
| CAUC/STN | Bloqueios para recebimento de repasses federais |
| DataJud (CNJ) | Ações judiciais de fornecedores por inadimplência |
| TCU | Achados críticos de auditoria |

## Status atual

🚧 **Fase 0 em desenvolvimento** — Relatório público dos 223 municípios da Paraíba

- [ ] Coleta SICONFI (223 municípios PB, 2020–2024)
- [ ] Coleta PNCP (licitações PB, 2023–2025)
- [ ] Coleta CAUC
- [ ] Coleta DataJud
- [ ] Cálculo do score
- [ ] Mapa coroplético interativo
- [ ] Relatório narrativo público
- [ ] App Streamlit com busca

## Como rodar localmente

```bash
git clone https://github.com/SEU_USUARIO/solvelicita.git
cd solvelicita
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
streamlit run app/main.py
