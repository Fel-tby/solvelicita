# Metodologia do Score de Solvência

**Versão:** 5.0  
**Última atualização:** Fevereiro/2026  
**Aviso:** Score baseado exclusivamente em dados oficiais declarados pelo próprio
município ao Tesouro Nacional (SICONFI/FINBRA) e ao Governo Federal (CAUC/STN). Qualquer
questionamento sobre os dados deve ser direcionado às fontes originais.

---

## Fórmula

    S = 22·f(Eorcam) + 18·g(Rrestos) + 14·Qsiconfi + 16·(1 - Ccauc)
      + 20·h(Scaixa) + 10·i(Autonomia)

O score é expresso em pontos (0–100). Os seis indicadores cobrem as dimensões
de execução fiscal, dívida herdada, transparência, risco de bloqueio federal,
saúde patrimonial de caixa e capacidade de geração de receita própria.

---

## Variáveis

| Variável     | Fonte        | O que mede                                            | Peso |
|--------------|--------------|-------------------------------------------------------|------|
| `Eorcam`     | SICONFI      | Execução orçamentária média 2020–2024                 | 22%  |
| `Rrestos`    | SICONFI      | Restos a pagar não processados / receita realizada    | 18%  |
| `Qsiconfi`   | SICONFI      | % de anos com RREO entregue (2020–2024)               | 14%  |
| `Ccauc`      | CAUC/STN     | Gravidade das pendências para recebimento federal     | 16%  |
| `Scaixa`     | FINBRA/DCA   | Saldo de caixa líquido médio / receita corrente       | 20%  |
| `Autonomia`  | FINBRA/DCA   | Receita tributária própria / receita corrente         | 10%  |

---

## Curvas de pontuação (limiares fixos)

Em vez de comparar municípios entre si (normalização relativa), o score usa
**limiares absolutos** — regras fixas baseadas em padrões de gestão fiscal
saudável. Isso evita que um município mal gerido pareça "bom" apenas por ser
melhor que os vizinhos.

### Eorcam — Execução Orçamentária (peso 22%)

Mede se o município arrecada o que planejou. A zona saudável é entre 90% e
105% — acima disso, geralmente indica emendas ou transferências extraordinárias
que não se repetem todo ano.

| Execução (%)   | Pontuação                  | Interpretação                       |
|----------------|----------------------------|-------------------------------------|
| ≥ 90% e ≤ 105% | 1.0 (máximo)               | Gestão precisa e previsível         |
| 105% – 120%    | decaimento linear 1.0→0.5  | Excesso por verba extraordinária    |
| > 120%         | 0.5 (teto)                 | Arrecadação anômala, não sustentável|
| 70% – 90%      | proporcional 0.0→1.0       | Zona de atenção                     |
| ≤ 70%          | 0.0 (zero)                 | Colapso de arrecadação              |

### Rrestos — Restos a Pagar Não Processados (peso 18%)

Mede o calote herdado: quanto da receita atual já está comprometida para pagar
dívidas do passado. É o melhor preditor de calote futuro disponível nos dados
de fluxo fiscal.

| Rrestos / Receita | Pontuação             | Interpretação                  |
|-------------------|-----------------------|--------------------------------|
| 0%                | 1.0 (máximo)          | Sem dívida herdada             |
| 0% – 3%           | decaimento suave      | Faixa aceitável                |
| 3% – 10%          | decaimento quadrático | Zona de risco crescente        |
| ≥ 10%             | 0.0 (zero)            | Dívida crítica para fornecedor |

**Tratamento especial:**
- `Rrestos` ausente → mediana estadual do período (comportamento neutro)
- `Rrestos` negativo → clampado a 0% + flag `dado_suspeito = True` no output  
  (valores negativos indicam cancelamento de empenhos sem liquidação — tratados
  como 0% por conservadorismo, mas sinalizados para análise combinada com `Scaixa`)

### Qsiconfi — Qualidade de Entrega (peso 14%)

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

### Ccauc — Risco de Bloqueio Federal (peso 16%)

Mede a **gravidade** das pendências do município no CAUC (Cadastro Único de
Convênios). A penalização é definida pela gravidade, não pela quantidade de
pendências. Pendências graves indicam que o Governo Federal já identificou risco
fiscal real e bloqueou repasses.

**Gatilho punitivo:** qualquer pendência grave zera a contribuição do CAUC,
independente dos demais indicadores.

| Tipo de pendência | Exemplos                                               | Impacto                               |
|-------------------|--------------------------------------------------------|---------------------------------------|
| **Grave**         | RFB, PGFN, CADIN, SISTN Dívida, LRF Executivo, TCU, CGU | `Ccauc = 1.0` → contribuição = 0   |
| **Moderada**      | FGTS, TST, SIOPS, SIOPE, LRF Legislativo               | penalidade proporcional, teto 0.5     |
| **Leve**          | pendências complementares e de reporte                 | penalidade mínima                     |
| **Regular**       | sem pendências                                         | `Ccauc = 0.0` → contribuição = 16 pts |

### Scaixa — Saldo de Caixa Líquido (peso 20%)

Calculado a partir dos dados anuais do FINBRA/DCA (Declaração de Contas Anuais,
STN). Mede a saúde patrimonial financeira do município: a diferença entre o
**Ativo Financeiro** (caixa, aplicações, recebíveis de curto prazo) e o
**Passivo Financeiro** (obrigações exigíveis imediatas), dividida pela receita
corrente líquida do mesmo exercício.

    Scaixa = (Ativo Financeiro - Passivo Financeiro) / Receita Corrente

É o único indicador que captura o **acúmulo histórico** de desequilíbrios —
diferentemente dos demais, que medem fluxos anuais. Um `Scaixa` negativo com
`Rrestos` zerado é o sinal mais confiável de cancelamento contábil de empenhos
sem liquidação efetiva.

A pontuação usa **limiares fixos absolutos** — não compara municípios entre si.

| Scaixa (médio 2020–2024) | Pontuação        | Interpretação                      |
|--------------------------|------------------|------------------------------------|
| ≥ 0.20                   | 1.00 (máximo)    | Folga patrimonial sólida           |
| 0.10 – 0.20              | 0.75             | Folga razoável                     |
| 0.00 – 0.10              | linear 0.50→0.75 | Ponto neutro a positivo            |
| -0.50 – 0.00             | quadrático 0→0.50| Passivo maior que ativo            |
| ≤ -0.50                  | 0.00 + ⚑        | Anomalia — `dado_suspeito = True`  |


### Autonomia — Receita Tributária Própria (peso 10%)

Calculado a partir do FINBRA/DCA. Mede a proporção da receita corrente que
o município gera por conta própria (IPTU, ISS, ITBI e taxas), sem depender de
transferências federais ou estaduais.

Municípios com autonomia alta são mais resilientes a cortes de repasse e têm
maior capacidade de honrar compromissos independentemente do ciclo político federal.

A pontuação usa uma **curva sigmoid calibrada por porte populacional** — municípios
pequenos têm referência diferente de municípios grandes, pois a base tributária
própria cresce com o tamanho. Os parâmetros foram calibrados com os dados reais
da PB (2020–2024) e devem ser revistos anualmente.

| Porte         | População         |
|---------------|-------------------|
| Micro         | < 10.000 hab      |
| Pequeno       | 10.000 – 50.000   |
| Médio         | 50.000 – 200.000  |
| Grande        | > 200.000 hab     |


---

## Classificação de risco

| Score  | Classificação  | Significado operacional                        |
|--------|----------------|------------------------------------------------|
| 75–100 | 🟢 Risco Baixo | Saúde fiscal sólida, sem bloqueios graves      |
| 55–74  | 🟡 Risco Médio | Avaliar pendências antes de participar         |
| 35–54  | 🔴 Risco Alto  | Exigir garantias contratuais                   |
| 0–34   | ⛔ Crítico     | Histórico grave — risco elevado de atraso      |
| —      | ⚫ Sem Dados   | SICONFI não enviado — risco não calculável     |

---

## Tratamento de dados ausentes

| Situação                               | Comportamento                                                  |
|----------------------------------------|----------------------------------------------------------------|
| Município sem SICONFI                  | Score não calculado — classificado como ⚫ Sem Dados           |
| `Rrestos` ausente em algum ano         | Mediana estadual do período (comportamento neutro)             |
| `Rrestos` negativo (dado suspeito)     | Clampado a 0% + flag `dado_suspeito = True` no output          |
| Município ausente no CAUC              | Pior caso (`Ccauc = 1.0`) — conservador                        |
| `Scaixa` ou `Autonomia` sem DCA        | Contribuição zerada — penaliza ausência de transparência       |

---

## Pipeline de cálculo

```
siconfi.py          → data/processed/siconfi_rreo_pb.csv
siconfi_processor.py → data/processed/siconfi_indicadores_pb.csv
cauc.py             → data/processed/cauc_situacao_pb.csv
dca.py              → data/processed/dca_indicadores_pb.csv
solvency.py         → data/outputs/score_municipios_pb.csv
```

O `solvency.py` une todos os indicadores e aplica a fórmula final.
O `dca.py` coleta e normaliza os dados do FINBRA/DCA (Scaixa e Autonomia).

---

## Limitações

- Score mede **capacidade estrutural de pagar**, não comportamento diário de
  fluxo de caixa
- Dados SICONFI e DCA são autodeclarados pelo município — qualidade varia
- CAUC é um snapshot da data de coleta — pode mudar entre a consulta e a
  assinatura do contrato
- `Scaixa` negativo com `Rrestos` zerado pode indicar cancelamento contábil
  de empenhos (limpeza de saldo sem liquidação) — flag `dado_suspeito` sinaliza
  os casos mais evidentes, mas a detecção completa requer as colunas de
  cancelamento do Anexo 07 do SICONFI (melhoria prevista)
- Não substitui due diligence jurídica para contratos de alto valor
- Fase 0 cobre apenas municípios da Paraíba (223 municípios)
