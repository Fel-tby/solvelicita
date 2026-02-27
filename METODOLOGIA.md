# Metodologia do Score de Solvência

**Versão:** 4.0  
**Última atualização:** Fevereiro/2026  
**Aviso:** Score baseado exclusivamente em dados oficiais declarados pelo próprio
município ao Tesouro Nacional (SICONFI) e ao Governo Federal (CAUC/STN). Qualquer
questionamento sobre os dados deve ser direcionado às fontes originais.

---

## Fórmula

    S = 31·f(Eorcam) + 25·g(Rrestos) + 19·Qsiconfi + 25·(1 - Ccauc)

O score é expresso em pontos (0–100). Os quatro indicadores cobrem as dimensões
de saúde fiscal, histórico de dívida herdada, transparência orçamentária e
risco de bloqueio federal — todos verificáveis em fontes públicas abertas.

---

## Variáveis

| Variável   | Fonte    | O que mede                                        | Peso |
|------------|----------|---------------------------------------------------|------|
| `Eorcam`   | SICONFI  | Execução orçamentária média 2020–2024             | 31%  |
| `Rrestos`  | SICONFI  | Restos a pagar não processados / receita realizada| 25%  |
| `Qsiconfi` | SICONFI  | % de anos com RREO entregue (2020–2024)           | 19%  |
| `Ccauc`    | CAUC/STN | Gravidade das pendências para recebimento federal | 25%  |

---

## Curvas de pontuação (limiares fixos)

Em vez de comparar municípios entre si (normalização relativa), o score usa
**limiares absolutos** — regras fixas baseadas em padrões de gestão fiscal
saudável. Isso evita que um município mal gerido pareça "bom" apenas por ser
melhor que os vizinhos.

### Eorcam — Execução Orçamentária (peso 31%)

Mede se o município arrecada o que planejou. A zona saudável é entre 90% e
105% — acima disso, geralmente indica emendas ou transferências extraordinárias
que não se repetem todo ano.

| Execução (%)    | Pontuação                   | Interpretação                      |
|-----------------|-----------------------------|------------------------------------|
| ≥ 90% e ≤ 105%  | 1.0 (máximo)                | Gestão precisa e previsível        |
| 105% – 120%     | decaimento linear 1.0→0.5   | Excesso por verba extraordinária   |
| > 120%          | 0.5 (teto)                  | Arrecadação anômala, não sustentável|
| 70% – 90%       | proporcional 0.0→1.0        | Zona de atenção                    |
| ≤ 70%           | 0.0 (zero)                  | Colapso de arrecadação             |

### Rrestos — Restos a Pagar Não Processados (peso 25%)

Mede o calote herdado: quanto da receita atual já está comprometida para pagar
dívidas do passado. É o melhor preditor de calote futuro disponível nos dados
fiscais.

| Rrestos / Receita | Pontuação             | Interpretação                  |
|-------------------|-----------------------|--------------------------------|
| 0%                | 1.0 (máximo)          | Sem dívida herdada             |
| 0% – 3%           | decaimento suave      | Faixa aceitável                |
| 3% – 10%          | decaimento quadrático | Zona de risco crescente        |
| ≥ 10%             | 0.0 (zero)            | Dívida crítica para fornecedor |

**Tratamento especial:**
- `Rrestos` ausente → mediana estadual do período (comportamento neutro)
- `Rrestos` negativo → clampado a 0% + flag `dado_suspeito = True` no output
  (valores negativos são tecnicamente impossíveis — indicam estorno ou erro de
  lançamento no SICONFI; tratados como 0% por conservadorismo)

### Qsiconfi — Qualidade de Entrega (peso 19%)

Proporção de anos (2020–2024) em que o município enviou o RREO ao Tesouro
Nacional. Município que não entrega contas não pode ser avaliado — e quem
esconde dados geralmente tem algo a esconder.

| Anos entregues (de 5) | Pontuação |
|-----------------------|-----------|
| 5                     | 1.0       |
| 4                     | 0.8       |
| 3                     | 0.6       |
| 2                     | 0.4       |
| 1                     | 0.2       |
| 0                     | 0.0       |

### Ccauc — Risco de Bloqueio Federal (peso 25%)

Mede a **gravidade** das pendências do município no CAUC (Cadastro Único de
Convênios). A penalização é definida pela gravidade, não pela quantidade de
pendências. Pendências graves indicam que o Governo Federal já identificou risco
fiscal real e bloqueou repasses.

**Gatilho punitivo:** qualquer pendência grave zera a contribuição do CAUC,
independente dos demais indicadores.

| Tipo de pendência | Exemplos                                          | Impacto                              |
|-------------------|---------------------------------------------------|--------------------------------------|
| **Grave**         | RFB, PGFN, CADIN, SISTN Dívida, LRF Executivo, TCU, CGU | `Ccauc = 1.0` → contribuição = 0 |
| **Moderada**      | FGTS, TST, SIOPS, SIOPE, LRF Legislativo          | penalidade proporcional, teto 0.5    |
| **Leve**          | pendências complementares e de reporte            | penalidade mínima                    |
| **Regular**       | sem pendências                                    | `Ccauc = 0.0` → contribuição = 25 pts|

---

## Classificação de risco

| Score   | Classificação   | Significado operacional                        |
|---------|-----------------|------------------------------------------------|
| 75–100  | 🟢 Risco Baixo  | Saúde fiscal sólida, sem bloqueios graves      |
| 55–74   | 🟡 Risco Médio  | Avaliar pendências antes de participar         |
| 35–54   | 🔴 Risco Alto   | Exigir garantias contratuais                   |
| 0–34    | ⛔ Crítico      | Histórico grave — risco elevado de atraso      |
| —       | ⚫ Sem Dados    | SICONFI não enviado — risco não calculável     |

---

## Tratamento de dados ausentes

| Situação                            | Comportamento                                          |
|-------------------------------------|--------------------------------------------------------|
| Município sem SICONFI               | Score não calculado — classificado como ⚫ Sem Dados   |
| `Rrestos` ausente em algum ano      | Mediana estadual do período (comportamento neutro)     |
| `Rrestos` negativo (dado suspeito)  | Clampado a 0% + flag `dado_suspeito = True` no output  |
| Município ausente no CAUC           | Pior caso (`Ccauc = 1.0`) — conservador                |

---

## Limitações

- Score mede **capacidade estrutural de pagar**, não comportamento diário de
  fluxo de caixa
- Dados SICONFI são autodeclarados pelo município — qualidade varia
- CAUC é um snapshot da data de coleta — pode mudar entre a consulta e a
  assinatura do contrato
- Não substitui due diligence jurídica para contratos de alto valor
- Fase 0 cobre apenas municípios da Paraíba (223 municípios)
