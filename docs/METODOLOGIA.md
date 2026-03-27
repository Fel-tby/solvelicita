# Metodologia do Score de Solvência

**Versão:** 7.0  
**Última atualização:** Março/2026  
**Aviso:** Score baseado exclusivamente em dados oficiais declarados pelo próprio município ao Tesouro Nacional (SICONFI/RREO/RGF e FINBRA/DCA) e ao Governo Federal (CAUC/STN). Qualquer questionamento sobre os dados deve ser direcionado às fontes originais.

> A evidência empírica de que o score discrimina risco real está documentada separadamente em [VALIDACAO.md](./VALIDACAO.md).

---

## Objetivo

SolveLicita responde à pergunta: **"Essa prefeitura tem capacidade fiscal de honrar seus contratos?"**

O score mede a **capacidade estrutural de solvência de curto a médio prazo** de municípios brasileiros, com horizonte de avaliação de 12 a 24 meses, compatível com o ciclo de contratos públicos de fornecimento, serviços continuados e obras.

Não é um modelo de previsão de inadimplência pontual. É um score de risco relativo baseado em múltiplas dimensões fiscais, construído exclusivamente com dados públicos.

---

## Fórmula

```
S = 35·f(Lliq) + 10·(1 − Ccauc) + 15·g(Eorcam)
  + 15·Qsiconfi + 10·h(Autonomia) + 15·i(RPproc)
```

O score é expresso em pontos (0–100).

---

## Variáveis

| Variável | Fonte | O que mede | Peso | Frequência |
|---|---|---|---|---|
| `Lliq` | RGF Anexo 05 (SICONFI) | Liquidez líquida: DCL pós-RP excl. RPPS / Receita Realizada | 35% | Bimestral/Sem. |
| `Ccauc` | CAUC/STN | Gravidade das pendências para recebimento federal | 10% | Diária |
| `Eorcam` | RREO Anexo 01 (SICONFI) | Execução orçamentária média ponderada por recência | 15% | Bimestral/Sem. |
| `Qsiconfi` | RREO histórico | % de anos com RREO entregue (2020–2025) + cap duro | 15% | Histórico |
| `Autonomia` | DCA/FINBRA | Receita tributária própria / receita corrente | 10% | Anual |
| `RPproc` | RREO Anexo 07 (SICONFI) | Cronicidade de restos a pagar liquidados não pagos | 15% | Bimestral/Sem. |

---

## Variável principal: Liquidez Líquida (Lliq)

### Definição

```
Lliq = (DCL_total_pós_RP − DCL_RPPS_pós_RP) / Receita_Realizada
```

Todos os componentes são extraídos do **RGF Anexo 05** (Demonstrativo da Disponibilidade de Caixa) do período mais recente entregue pelo município.

- `DCL_total_pós_RP`: Disponibilidade de Caixa Líquida após dedução dos Restos a Pagar totais (processados + não processados), extraído da linha `DisponibilidadeDeCaixaLiquidaAposRP`, conta `TOTAL (IV) = (I + II + III)`
- `DCL_RPPS_pós_RP`: porção atribuída ao RPPS — subtraída para evitar distorção do regime previdenciário próprio, que tem caixa vinculado de uso restrito
- `Receita_Realizada`: receita total realizada do exercício, extraída do RREO Anexo 01 (`ReceitasExcetoIntraOrcamentarias`, coluna `Até o Bimestre (c)`)

### Por que DCL pós-RP e não Caixa Bruto

A versão anterior (v5.x) usava Saldo de Caixa (DCA) e Restos a Pagar (RREO Anexo 07) como variáveis independentes. Esses dois indicadores são altamente correlacionados negativamente por construção contábil: quando RP Processados sobem, o caixa líquido disponível cai. Tratá-los separadamente causava **duplicação de sinal** — o modelo penalizava duas vezes o mesmo fenômeno. A fusão em `Lliq` via RGF Anexo 05 elimina a multicolinearidade, usa a mesma fonte temporal para ambos os componentes, e eleva a frequência do indicador mais importante de anual (DCA) para bimestral/semestral (RGF).

### Regime de entrega do RGF por porte

| Porte | Frequência RGF | Janela de dado aceitável |
|---|---|---|
| > 50.000 hab. | Bimestral (Q) | ≤ 90 dias |
| ≤ 50.000 hab. | Semestral (S) | ≤ 210 dias |

Quando ambas as periodicidades estão disponíveis para o mesmo exercício, o dado **quadrimestral (Q) tem prioridade** sobre o semestral (S).

### Fallback pré-RPNP (lliq_parcial)

Quando o município entregou RGF Anexo 05 mas **sem a coluna pós-RPNP** (padrão anterior a certas versões do SICONFI), o sistema usa a coluna pré-RPNP como proxy:

```
lliq_parcial = DCL_bruta_pré_RP / Receita_Realizada
```

O município recebe flag `lliq_parcial = True` no output. Esta versão **superestima a liquidez** por não deduzir RPNP — o modelo compensa com penalidade adicional de 5 pts no score.

### Confidence decay (defasagem de dado)

Quando o RGF Anexo 05 mais recente disponível está **fora da janela aceitável** para o porte:

- Aplica-se penalidade proporcional sobre a contribuição de `Lliq` no score final (`decay_fator < 1.0`)
- O município recebe flag `dado_defasado = True` e `dias_atraso` no output
- O fator de decay é calculado como: `decay = max(0, 1 − (dias_atraso − janela) / 365)`

### Curva de pontuação

| Lliq | Pontuação | Interpretação |
|---|---|---|
| ≥ 0.35 | 1.00 (máximo) | Folga de liquidez sólida |
| 0.10 – 0.35 | linear 0.60→1.00 | Liquidez razoável |
| 0.00 – 0.10 | linear 0.35→0.60 | Liquidez positiva, mas estreita |
| −0.50 – 0.00 | linear 0.00→0.35 | Passivo imediato maior que caixa |
| < −0.50 | 0.00 + ⚑ | Anomalia — `dado_suspeito = True` |

Valores `Lliq < −0.50` são sinalizados como `dado_suspeito = True` e têm score calculado com capping em −0.50. Possível causa: distorção de RPPS, cancelamento contábil de empenhos sem liquidação, ou erro de envio ao SICONFI.

---

## Ccauc — Risco de Bloqueio Federal (peso 10%)

Mede a **gravidade** das pendências do município no CAUC (Cadastro Único de Convênios). É o único indicador genuinamente independente das fontes SICONFI: não é autodeclarado pelo município, é verificado externamente pelo Governo Federal.

| Tipo de pendência | Exemplos | Impacto |
|---|---|---|
| **Grave** | RFB, PGFN, CADIN, SISTN Dívida, LRF Executivo, TCU, CGU | `Ccauc = 1.0` → 0 pts |
| **Moderada** | FGTS, TST, SIOPS, SIOPE, LRF Legislativo, SICONV | Penalidade proporcional, teto 0.5 |
| **Leve** | Pendências de reporte (SICONFI, MCASP, PCASP) | Penalidade mínima |
| **Regular** | Sem pendências | `Ccauc = 0.0` → 10 pts |

A coleta é feita via Portal de Dados Abertos do Tesouro (CKAN) — snapshot nacional filtrado para municípios da PB.

---

## Eorcam — Execução Orçamentária (peso 15%)

Mede se o município arrecada o que planejou. Usa **média ponderada por recência** sobre os exercícios com RREO entregue (2020–2025):

| Exercício | Peso relativo |
|---|---|
| 2025 | 40% |
| 2024 | 25% |
| 2023 | 20% |
| 2022 | 10% |
| 2021 | 5% |
| 2020 | 0% (reserva histórica, não entra na média ponderada) |

A zona saudável é entre 90% e 105%.

| Execução (%) | Pontuação | Interpretação |
|---|---|---|
| ≥ 90% e ≤ 105% | 1.0 (máximo) | Gestão precisa e previsível |
| 105% – 120% | decaimento linear 1.0→0.5 | Excesso por verba extraordinária |
| > 120% | 0.5 (teto) | Arrecadação anômala, não sustentável |
| 70% – 90% | proporcional 0.0→1.0 | Zona de atenção |
| ≤ 70% | 0.0 | Colapso de arrecadação |

---

## Qsiconfi — Qualidade e Transparência (peso 15% + cap duro)

Proporção de anos (2020–2025) em que o município enviou o RREO ao Tesouro Nacional.

### Pontuação numérica

| Anos entregues (de 6) | Pontuação |
|---|---|
| 6 | 1.0 |
| 5 | 0.83 |
| 4 | 0.67 |
| 3 | 0.50 |
| 2 | 0.33 |
| 1 | 0.17 |
| 0 | 0.0 |

### Cap duro de classificação

Independente do score numérico calculado pelos demais indicadores:

| Anos entregues | Cap máximo de classificação |
|---|---|
| ≥ 4 de 6 | Sem restrição |
| 3 de 6 | Teto: 🟡 Risco Médio |
| ≤ 2 de 6 | Teto: 🔴 Risco Alto |
| 0 de 6 | ⚫ Sem Dados |

**Justificativa:** dado ausente não é sinal neutro — é risco não quantificável, que em gestão de crédito equivale a rebaixamento automático.

---

## Autonomia — Receita Tributária Própria (peso 10% + flag)

Calculado a partir do FINBRA/DCA. Mede a proporção da receita corrente gerada autonomamente pelo município (IPTU, ISS, ITBI e taxas), sem depender de repasses federais ou estaduais.

**Flag de alerta:** municípios com `Autonomia < 8% da RCL` recebem flag `autonomia_critica = True` no output, independente do score. Municípios nessa faixa são completamente dependentes do FPM, que pode variar 20–30% entre meses.

Pontuação via **curva sigmoid calibrada por porte populacional**:

| Porte | População |
|---|---|
| Micro | < 10.000 hab. |
| Pequeno | 10.000 – 50.000 |
| Médio | 50.000 – 200.000 |
| Grande | > 200.000 hab. |

---

## RPproc — Cronicidade de Restos a Pagar (peso 15% + cap duro)

Mede se o município tem **padrão crônico de não pagamento** de despesas já liquidadas.

### Indicador base: rproc_pct

```
rproc_pct = RestosAPagarProcessadosENaoProcessadosLiquidadosAPagar / Receita_Realizada
```

Extraído do **RREO Anexo 07**, `cod_conta = RestosAPagarProcessadosENaoProcessadosLiquidadosAPagar`, coluna `Saldo e = (a+ b) - (c + d)`, linha `TOTAL (III) = (I + II)`.

### n_anos_cronicos

Contagem de anos (sobre todos os exercícios 2020–2025 com RREO entregue) em que `rproc_pct > 3%`. Apenas anos **anteriores a T** são considerados no cálculo — sem uso de informação futura.

### Curva de pontuação

| n_anos_cronicos | rproc_norm | Interpretação |
|---|---|---|
| 0 | 1.00 | Nenhum padrão de atraso |
| 1 | 0.75 | Episódico |
| 2 | 0.50 | Recorrente |
| 3 | 0.30 | Preocupante |
| 4 | 0.10 | Grave |
| 5 ou 6 | 0.00 | Crônico estrutural |

### Cap duro de classificação

Municípios com `n_anos_cronicos ≥ 5` têm classificação máxima **travada em 🟡 Risco Médio**, independente do score numérico.

---

## Classificação de risco

| Score | Classificação |
|---|---|
| 80–100 | 🟢 Risco Baixo |
| 60–79 | 🟡 Risco Médio |
| 40–59 | 🔴 Risco Alto |
| 0–39 | ⛔ Crítico |
| — | ⚫ Sem Dados |

**Caps duros independentes do score numérico:**
- Transparência (`Qsiconfi`): ver seção acima
- Cronicidade de RP (`RPproc`): `n_anos_cronicos ≥ 5` → teto 🟡 Risco Médio

---

## Tratamento de dados ausentes

| Situação | Comportamento |
|---|---|
| Município sem RREO (0 anos) | Score não calculado — ⚫ Sem Dados |
| RGF Anexo 05 fora da janela temporal | Confidence decay: fator proporcional em `Lliq` + flag `dado_defasado` |
| Apenas coluna pré-RPNP disponível | `lliq_parcial = True` + penalidade de 5 pts |
| `Lliq` anômalo (< −0.50) | Capping em −0.50 + flag `dado_suspeito` |
| `rproc_pct` indisponível em algum ano | Ano excluído do cômputo de `n_anos_cronicos` |
| Município ausente no CAUC | Pior caso (`Ccauc = 1.0`) — conservador |
| DCA ausente (sem Autonomia) | Contribuição = 0 — penaliza ausência |

---

## Limitações

- `Lliq` mede liquidez estrutural declarada — não substitui análise de fluxo de caixa diário ou due diligence jurídica
- Dados SICONFI são autodeclarados pelo município — qualidade varia; `Qsiconfi` penaliza historicamente inconsistentes
- CAUC é snapshot da data de coleta — pode mudar a qualquer momento; recoletar antes de qualquer decisão é recomendado
- DCA/FINBRA tem defasagem anual estrutural (~14 meses no pior caso) — afeta `Autonomia` apenas; `Lliq` usa RGF com defasagem máxima de 90–210 dias
- `Lliq` negativo extremo pode indicar distorção de RPPS ou cancelamento contábil de empenhos — flag `dado_suspeito` sinaliza, mas detecção completa requer auditoria manual do Balanço Patrimonial
- `rproc_pct` não distingue municípios que quitaram RP por pagamento real daqueles que quitaram por cancelamento contábil
- Fase 0 cobre apenas municípios da Paraíba (223 municípios) — curvas de Autonomia calibradas para esse universo
