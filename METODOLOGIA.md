# Metodologia do Score de Solvência

**Versão:** 3.0  
**Última atualização:** Fevereiro/2026  
**Aviso:** Score baseado exclusivamente em dados oficiais declarados pelo próprio
município ao Tesouro Nacional (SICONFI) e ao CNJ (DataJud). Qualquer
questionamento sobre os dados deve ser direcionado às fontes originais.

---

## Fórmula

    S = 15·f(Eorcam) + 25·g(Rrestos) + 15·Qsiconfi
      + 20·(1 - Ccauc) + 10·(1 - Jdatajud)
      +  7·(1 - Atcu)  +  3·(1 - Sceis)

O score é expresso em pontos (0–100). Na Fase 0, com DataJud, TCU e CEIS/CNEP
pendentes, o máximo atingível é 75 pontos. Os 25 pontos restantes serão
incorporados nas próximas versões.

---

## Variáveis

| Variável   | Fonte       | O que mede                                          | Peso |
|------------|-------------|-----------------------------------------------------|------|
| `Eorcam`   | SICONFI     | Execução orçamentária média 2020–2024               | 15%  |
| `Rrestos`  | SICONFI     | Restos a pagar não processados / receita realizada  | 25%  |
| `Qsiconfi` | SICONFI     | % de anos com RREO entregue (2020–2024)             | 15%  |
| `Ccauc`    | CAUC/STN    | Gravidade das pendências para recebimento federal   | 20%  |
| `Jdatajud` | DataJud/CNJ | Ações judiciais de fornecedores por inadimplência   | 10%  |
| `Atcu`     | TCU         | Achados críticos de auditoria nos últimos 3 anos    |  7%  |
| `Sceis`    | CEIS/CNEP   | Órgão ou gestor com sanções ativas                  |  3%  |

---

## Curvas de pontuação (limiares fixos)

Em vez de comparar municípios entre si (normalização relativa), o score usa
**limiares absolutos** — regras fixas baseadas em padrões de gestão fiscal
saudável. Isso evita que um município mal gerido pareça "bom" apenas por ser
melhor que os vizinhos.

### Eorcam — Execução Orçamentária (peso 15%)

Mede se o município arrecada o que planejou. A zona saudável é entre 90% e
105% — acima disso, geralmente indica emendas ou transferências extraordinárias
que não se repetem todo ano.

| Execução (%) | Pontuação | Interpretação |
|---|---|---|
| ≥ 90% e ≤ 105% | 1.0 (máximo) | Gestão precisa e previsível |
| 105% – 120% | decaimento linear 1.0→0.5 | Excesso por verba extraordinária |
| > 120% | 0.5 (teto) | Arrecadação anômala, não sustentável |
| 70% – 90% | proporcional 0.0→1.0 | Zona de atenção |
| ≤ 70% | 0.0 (zero) | Colapso de arrecadação |

### Rrestos — Restos a Pagar Não Processados (peso 25%)

Mede o calote herdado: quanto da receita atual já está comprometida para pagar
dívidas do passado. É o melhor preditor do calote futuro disponível nos dados
fiscais.

| Rrestos / Receita | Pontuação | Interpretação |
|---|---|---|
| 0% | 1.0 (máximo) | Sem dívida herdada |
| 0% – 3% | decaimento suave | Faixa aceitável |
| 3% – 10% | decaimento quadrático | Zona de risco crescente |
| ≥ 10% | 0.0 (zero) | Dívida crítica para fornecedor |

### Qsiconfi — Qualidade de Entrega (peso 15%)

Proporção de anos (2020–2024) em que o município enviou o RREO ao Tesouro
Nacional. Município que não entrega contas não pode ser avaliado — e quem
esconde dados geralmente tem algo a esconder.

| Anos entregues | Pontuação |
|---|---|
| 5 de 5 | 1.0 |
| 4 de 5 | 0.8 |
| 3 de 5 | 0.6 |
| ... | ... |
| 0 de 5 | 0.0 |

### Ccauc — Bloqueio Federal (peso 20%)

Mede a gravidade das pendências do município no CAUC (Cadastro Único de
Convênios). Pendências graves indicam que o Governo Federal já identificou risco
fiscal real e bloqueou repasses.

**Gatilho punitivo:** qualquer pendência grave zera a contribuição do CAUC,
independente dos demais indicadores.

| Tipo de pendência | Exemplos | Impacto |
|---|---|---|
| **Grave** | RPPS, RFB, PGFN, CADIN, SISTN Dívida, LRF Executivo | `Ccauc = 1.0` → contribuição = 0 |
| **Moderada** | FGTS, TST, SIOPS, SIOPE, LRF Legislativo | penalidade proporcional, teto 0.5 |
| **Leve** | SICONFI entrega, SIOPE complementar | penalidade mínima |
| **Regular** | sem pendências | `Ccauc = 0.0` → contribuição = 20 pts |

---

## Classificação de risco (Fase 0)

Os thresholds são calibrados para o máximo atingível de 75 pontos na Fase 0.

| Score | Classificação | Significado operacional |
|---|---|---|
| 65 – 75 | 🟢 Risco Baixo | Saúde fiscal sólida, sem bloqueios graves |
| 50 – 64 | 🟡 Risco Médio | Avaliar pendências antes de participar |
| 35 – 49 | 🔴 Risco Alto | Exigir garantias contratuais |
| 0 – 34 | ⛔ Crítico | Histórico grave — risco elevado de atraso |
| — | ⚫ Sem Dados | SICONFI não enviado — risco não calculável |

---

## Tratamento de dados ausentes

| Situação | Comportamento |
|---|---|
| Município sem SICONFI | Score não calculado — exibir aviso |
| Rrestos ausente em algum ano | Mediana estadual do período |
| Município ausente no CAUC | Pior caso (`Ccauc = 1.0`) — conservador |
| DataJud/TCU/CEIS pendentes | 25 pontos não atribuídos — aviso explícito |

---

## Limitações

- Score mede **capacidade estrutural de pagar**, não comportamento diário de
  fluxo de caixa
- Dados SICONFI são autodeclarados pelo município — qualidade varia
- CAUC é um snapshot — pode mudar entre a consulta e a assinatura do contrato
- Não substitui due diligence jurídica para contratos de alto valor
- Fase 0 cobre apenas municípios da Paraíba (223 municípios)
