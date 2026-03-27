# Validação do Score de Solvência

**Versão do score:** 7.0  
**Última atualização:** Março/2026  
**Referência cruzada:** [METODOLOGIA.md](./METODOLOGIA.md)

> Este documento registra a evidência empírica de que o score discrimina risco fiscal real.
> Não descreve como o score é calculado — isso está em METODOLOGIA.md.

---

## Estratégia

A validação usa **walk-forward por pares de anos consecutivos**: o score é calculado com dados de T0 e o desfecho observado é `rproc_pct` em T1. Isso replica a situação real de uso — o score prevê comportamento futuro a partir de informação presente, sem acesso a dados do período avaliado.

Desfecho binário adotado: `rproc_pct > 3%` em T1 = **evento crônico**. O limiar de 3% é o mesmo usado internamente pelo score para classificar `n_anos_cronicos`.

---

## Dados

| Elemento | Valor |
|---|---|
| Universo | 215 municípios da Paraíba |
| Período | 2020–2025 (6 ciclos) |
| Pares walk-forward válidos | 881 |
| Era Completa (com lliq) | 342 pares — 2023→2024 e 2024→2025 |
| Era Parcial (sem lliq) | 539 pares — 2020→2021, 2021→2022, 2022→2023 |
| Eventos crônicos na Era Completa | 81 (23,7%) |
| Eventos crônicos na Era Parcial | 185 (34,3%) |

**Componentes neutralizados no backtest:** CAUC (fixado em 0.0) e Autonomia (fixada em 0.5) não possuem série histórica no `siconfi_indicadores`. Os 20% de peso correspondentes são redistribuídos proporcionalmente entre os componentes ativos. O AUC reportado representa, portanto, o score operando com ~80% de sua informação disponível.

---

## Métricas de validação

Duas métricas são utilizadas, cada uma respondendo a uma pergunta distinta:

**Spearman** — *a ordenação do score corresponde à ordenação do risco real?*  
Correlação ordinal entre `score_T0` e `rproc_T1` sobre a escala contínua. Independe dos limiares de classificação.

**AUC-ROC** — *o score separa municípios que vão se tornar crônicos dos que não vão?*  
Probabilidade de que o score ordene corretamente um par aleatório (crônico vs não-crônico). AUC=0.50 equivale a aleatoriedade; AUC=1.0 equivale a separação perfeita.

---

## Resultados — Score completo

### Correlação de Spearman

| Par | n | r | p | Observação |
|---|---|---|---|---|
| 2020→2021 | 172 | −0.091 | 0.233 n.s. | ⚠ Ano atípico COVID — ver seção abaixo |
| 2021→2022 | 183 | −0.302 | <0.001 *** | |
| 2022→2023 | 182 | −0.362 | <0.001 *** | |
| 2023→2024 | 166 | −0.363 | <0.001 *** | |
| 2024→2025 | 178 | −0.337 | <0.001 *** | |
| **Era Parcial** | 539 | **−0.248** | <0.001 *** | |
| **Era Completa** | 342 | **−0.340** | <0.001 *** | |

Referência de magnitude: \|r\| < 0.10 fraco · 0.10–0.30 moderado · > 0.30 forte.

### AUC-ROC

| Era | n | Eventos crônicos | AUC |
|---|---|---|---|
| Era Parcial | 539 | 185 (34,3%) | **0.643** |
| Era Completa | 342 | 81 (23,7%) | **0.750** |

O AUC de 0.750 na era completa significa: dado um município que se tornou crônico e um que não se tornou, o score aponta o correto em 75% dos casos. Com 81 eventos positivos, o intervalo de confiança bootstrap estimado tem amplitude ~0.12 (IC 95%: ~0.69–0.81).

### Gradiente de risco — Era Completa

| Classe em T0 | n | Mediana rproc T1 | % crônicos em T1 |
|---|---|---|---|
| 🟢 Risco Baixo | 80 | 0,52% | 8,8% |
| 🟡 Risco Médio | 200 | 0,74% | 21,5% |
| 🔴 Risco Alto | 62 | 3,00% | **50,0%** |

O gradiente é monótono e sem inversões. Municípios classificados como Risco Alto têm **5,7× mais probabilidade** de se tornarem crônicos no ano seguinte do que municípios classificados como Risco Baixo.

---

## Análise de sensibilidade

### 1. Exclusão de 2020 como T0

O par 2020→2021 produz r=−0.091 (n.s.), quebrando a sequência de quatro pares consecutivos com r forte e significativo. A causa identificada são os repasses emergenciais da LC 173/2020 (COVID): municípios com perfil fiscal deteriorado receberam caixa atípico que inflou `eorcam` e `lliq` de 2020, superestimando o score para exatamente os municípios de maior risco real.

Rodando com `--excluir-t0 2020`:

| Era | AUC (com 2020) | AUC (sem 2020) | Delta |
|---|---|---|---|
| Era Parcial | 0.643 | **0.706** | +0.063 |
| Era Completa | 0.750 | 0.750 | 0.000 |

A era completa não é afetada (2020 não é T0 em nenhum par dela). O ganho de +0.063 na era parcial confirma que 2020 é o responsável pela diferença de AUC entre as duas eras — não a ausência de `lliq`. Sem o ruído de 2020, a era parcial também atinge AUC moderado (0.706).

**Implicação prática:** scores calculados com dados de 2020 como âncora têm maior incerteza. Isso afeta apenas municípios cujo `eorcam_ponderado` ainda carrega peso do exercício 2020 (i.e., municípios com poucos anos de histórico a partir de 2021).

### 2. Remoção de RPproc (`--sem-rproc`)

RPproc tem circularidade parcial com o desfecho: `n_anos_cronicos` é calculado a partir do histórico de `rproc_pct`, que é a mesma variável usada para definir o evento crônico em T1. Para quantificar o quanto o sinal depende dessa circularidade:

| Era | AUC com RPproc | AUC sem RPproc | Delta |
|---|---|---|---|
| Era Parcial | 0.643 | 0.547 | −0.096 |
| Era Completa | 0.750 | 0.642 | −0.108 |

O Spearman sem RPproc colapsa na era parcial: r=−0.093 (p=0.03, fraco), frente a r=−0.248 (***) com RPproc. Dois pontos se confirmam:

1. **RPproc carrega sinal real**, não apenas artefato de circularidade — se fosse puramente circular, o AUC sem RPproc seria próximo de 0.50 e o com RPproc seria artificialmente alto. O AUC sem RPproc na era completa (0.642) ainda discrimina moderadamente, indicando que `lliq`, `eorcam` e `qsiconfi` têm poder preditivo independente.

2. **A circularidade existe e é mensurável.** O delta de −0.108 na era completa representa a contribuição *conjunta* de sinal real e circularidade de RPproc. Não é possível separá-los com os dados disponíveis.

**Conclusão conservadora:** o AUC verdadeiro do score sem circularidade está entre 0.642 (sem RPproc) e 0.750 (com RPproc). O valor reportado em produção deve ser 0.750 com a nota de circularidade parcial.

---

## Erros extremos — Era Completa

### Falsos positivos (classificados como Alto/Crítico, rproc T1 < 1%)

Municípios penalizados pelo score mas que não materializaram risco no ano seguinte. Todos os oito casos têm score entre 55–60 — fronteira exata da classe Alto. Nenhum está no núcleo da classificação.

| Município | Score T0 | rproc T1 |
|---|---|---|
| Cajazeiras | 59.9 | 0.26% |
| Catingueira | 59.4 | 0.59% |
| Cacimba de Areia | 57.9 | 0.33% |
| Ibiara | 57.4 | 0.02% |
| Passagem | 57.0 | 0.58% |
| Riachão do Poço | 56.9 | 0.19% |
| Lastro | 55.6 | 0.09% |
| Fagundes | 55.4 | 0.48% |

A concentração na fronteira de classe é esperada estatisticamente — scores próximos ao limiar têm maior incerteza de classificação. Não há falsos positivos com score abaixo de 50.

### Falsos negativos (classificados como Baixo/Médio, rproc T1 > 5%)

Municípios que o score considerou razoavelmente saudáveis mas que deterioraram no ano seguinte. São o risco operacional mais relevante do produto.

| Município | Score T0 | rproc T1 | Diagnóstico |
|---|---|---|---|
| Poço de José de Moura | 90.2 | 5.73% | `lliq` alta em T0 mascarou deterioração abrupta |
| Poço Dantas | 79.7 | 11.18% | Mesmo padrão |
| Curral de Cima | 74.9 | 7.23% | Mesmo padrão |
| Monte Horebe | 68.6 | 6.17% | Mesmo padrão |
| Mogeiro | 66.0 | 6.09% | Mesmo padrão |
| Aparecida | 61.6 | 6.16% | Score já na fronteira |
| Bayeux | 60.2 | 5.49% | Score já na fronteira |
| Sobrado | 63.9 | 9.90% | Mesmo padrão |

O padrão dominante nos casos graves (score > 65) é liquidez positiva em T0 seguida de deterioração abrupta de RP em T1 — choque que nenhum modelo anual consegue antecipar sem dados infraanuais. A mitigação operacional recomendada é monitoramento trimestral de `rproc_pct` para municípios com score entre 70–90 que possuam histórico de `n_anos_cronicos ≥ 1`.

---

## Limitações da validação

1. **CAUC e Autonomia neutralizados.** 20% dos pesos rodaram como constante. O AUC do score completo com série histórica de CAUC e Autonomia é provavelmente superior ao reportado.

2. **Circularidade parcial de RPproc.** O delta mensurável é −0.108 de AUC (era completa). O AUC conservador do score sem esse componente é 0.642.

3. **Amostra de eventos pequena.** n=81 positivos na era completa implica IC bootstrap de amplitude ~0.12. Ajuste fino de pesos com base nesses resultados seria overfitting — os pesos v7.0 não foram otimizados sobre este backtest.

4. **2020 como T0 produz ruído estrutural** (COVID). O par 2020→2021 deve ser interpretado como dado contaminado, não como evidência contra o modelo. A análise sem 2020 (AUC parcial = 0.706) é mais representativa do desempenho esperado em anos normais.

5. **Horizonte de um ano.** O score foi validado para prever cronicidade no ano imediatamente seguinte. Para horizontes de 2+ anos, o poder preditivo não foi testado.

---

## Reprodutibilidade

```bash
# Resultado base
python src/backtest_validacao.py

# Sensibilidade: exclusão de ano atípico
python src/backtest_validacao.py --excluir-t0 2020

# Sensibilidade: isolamento da circularidade de RPproc
python src/backtest_validacao.py --sem-rproc

# Era completa isolada
python src/backtest_validacao.py --pares completa
```

Saídas geradas em `data/outputs/`:
- `backtest_pares.csv` — registro por par município×ano com todos os componentes
- `backtest_resumo.txt` — relatório estatístico completo
