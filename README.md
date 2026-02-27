# SolveLicita

> **"Essa prefeitura vai me pagar?"** — a pergunta que nenhuma plataforma de licitações responde.

## O problema

25% das PMEs brasileiras estão inadimplentes. Um dos fatores de risco oculto é fornecer para entes públicos que atrasam ou não pagam — mas não existe hoje uma ferramenta pública que avalie a solvência do comprador público *antes* da empresa investir tempo e recursos em participar da licitação.

## A solução

SolveLicita calcula um **Score de Solvência (0–100)** para municípios brasileiros, cruzando fontes de dados públicos oficiais:

| Fonte | O que mede |
|-------|------------|
| SICONFI (Tesouro Nacional) | Execução orçamentária, restos a pagar e saldo de caixa |
| CAUC/STN | Bloqueios para recebimento de repasses federais |
| FINBRA/DCA (STN) | Saldo de caixa líquido e autonomia tributária |
| PNCP | Histórico de compras públicas |

## Status atual

**Fase 1 concluída** — Score de solvência calculado para os 223 municípios da Paraíba

- [x] Tabela mestra de municípios PB (223 municípios)
- [x] Coleta SICONFI (223 municípios PB, 2020–2024)
- [x] Coleta CAUC (snapshot 24/02/2026)
- [x] Coleta PNCP (54.139 licitações, 220 municípios, 2023–2026)
- [x] Coleta FINBRA/DCA (saldo de caixa e autonomia tributária, 2020–2024)
- [x] Cálculo do score de solvência (6 indicadores, 100 pts)
- [x] Mapa coroplético interativo (Streamlit + Folium)
- [ ] Relatório narrativo público
- [ ] Expansão para demais estados

## Como rodar localmente

```bash
git clone https://github.com/Fel-tby/solvelicita.git
cd solvelicita
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Preparar dados geoespaciais (apenas uma vez)
python app/preparar_dados.py

# Rodar o dashboard
streamlit run app/main.py
```
