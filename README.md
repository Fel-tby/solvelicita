<p align="center">
  <img src="docs/assets/icons/solvelicita_github_header.svg" alt="SolveLicita Engine — Score de Solvência Municipal" width="100%">
</p>

<p align="center">
  <a href="https://github.com/Fel-tby/solvelicita-engine/actions/workflows/ci.yml"><img src="https://github.com/Fel-tby/solvelicita-engine/actions/workflows/ci.yml/badge.svg" alt="Engine CI"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white" alt="Python"></a>
  <a href="https://www.getdbt.com/"><img src="https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white" alt="dbt"></a>
  <a href="https://cloud.google.com/bigquery"><img src="https://img.shields.io/badge/BigQuery-669DF6?logo=googlebigquery&logoColor=white" alt="BigQuery"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/Licença-AGPL--3.0-blue.svg" alt="Licença AGPL-3.0"></a>
</p>

---

## Inteligência Fiscal e Solvência

Esta aplicação é o componente técnico responsável pelo calculo de risco da plataforma **SolveLicita**. Sua função é automatizar o ciclo completo de análise de risco fiscal de todos os municípios brasileiros, transformando dados públicos brutos em um Score de Solvência padronizado e acionável. Para isso, o pipeline executa um fluxo contínuo que coleta informações de múltiplas fontes oficiais via APIs, estrutura e consolida registros contábeis complexos através de camadas de modelagem SQL e aplica pós-processadores analíticos em Python para o cálculo final dos indicadores. Essa automação traduz relatórios técnicos dispersos em uma resposta direta sobre a capacidade real de pagamento de cada prefeitura, servindo como a base de dados fundamental para a plataforma web do SolveLicita.

---

## O score

Um **Score de Solvência (0–100)** calculado a partir de indicadores fiscais públicos, ponderados por relevância:

| Indicador | Fonte | Peso | O que mede |
|---|---|---|---|
| Liquidez Líquida | SICONFI / RGF Anexo 05 | **40%** | Caixa disponível após Restos a Pagar |
| RP Crônicos | SICONFI / RREO Anexo 07 | **20%** | Histórico de dívidas com fornecedores |
| Execução Orçamentária | SICONFI / RREO Anexo 01 | **15%** | Aderência entre receita prevista e realizada |
| Autonomia Tributária | FINBRA / DCA | **15%** | Dependência do FPM vs receita própria |
| Bloqueio Federal | CAUC / STN | **10%** | Pendências que bloqueiam repasses federais |
| Transparência Fiscal | SICONFI | **0%** | Cobertura/cap de dados contábeis entregues |

**Modulação por Confiabilidade (ICF SICONFI):**
A Transparência Fiscal (`qsiconfi`) deixou de ter peso numérico direto e opera como cobertura e cap duro de classificação de risco. Em seu lugar, a confiabilidade formal dos dados é integrada através do **Ranking da Qualidade da Informação Contábil e Fiscal (ICF)** do Tesouro Nacional. Municípios com menor confiabilidade (conceito B a E) sofrem uma redução proporcional de score nos componentes calculados via SICONFI (multiplicadores de **1,0** para conceito A, decaindo até **0,80** para conceito E ou ausência de ranking).

A fórmula detalhada, os multiplicadores de redução, as curvas de pontuação e as justificativas estão descritas em [`docs/METODOLOGIA.md`](docs/METODOLOGIA.md).

**Classificação:**

| Score | Classificação |
|---|---|
| ≥ 75 | 🟢 Risco Baixo |
| 55 – 74 | 🟡 Risco Médio |
| 35 – 54 | 🔴 Risco Alto |
| < 35 | ⛔ Crítico |
| — | ⚫ Sem Dados |

Além do score numérico, dois caps de classificação operam de forma independente: municípios com histórico de não entrega de dados não podem ser classificados como Risco Baixo, e municípios com padrão crônico de Restos a Pagar Processados têm teto em Risco Médio.

---

## Backtest do score

O score é validado por um backtest walk-forward reproduzível, versionado em [`src/analysis/backtest_validacao.py`](src/analysis/backtest_validacao.py): o score é calculado em um ano e comparado com o comportamento fiscal observado no ano seguinte, usando a ocorrência futura de Restos a Pagar crônicos como desfecho.

Na validação geral documentada, o modelo operacional apresentou:

| Métrica | Resultado |
|---|---:|
| Pares município-ano avaliados | **4.671** |
| AUC-ROC | **0.7443** |
| Spearman | **-0.3827** |

Os dois recortes podem ser executados diretamente no repositório:

```bash
python src/analysis/backtest_validacao.py --geral --excluir-t0 2020
python src/analysis/backtest_validacao.py --geral --sem-rproc --excluir-t0 2020
```

Resultados, limitações e testes de sensibilidade estão em [`docs/VALIDACAO.md`](docs/VALIDACAO.md).

---

## Arquitetura do pipeline

O fluxo opera com um modelo **BigQuery-first**. Os dados públicos são coletados das APIs oficiais, estruturados com `dbt`, enriquecidos por pós-processadores Python e consolidados. O resultado é publicado em snapshots históricos e sincronizado no Supabase para consumo pela aplicação web do SolveLicita.

```mermaid
flowchart LR
    PL(["pipeline.py"])

    subgraph INGEST ["1. Ingestão"]
        direction LR
        APIS["APIs públicas<br/>IBGE / SICONFI / DCA / CAUC / PNCP"]
        RAW[("BigQuery raw")]
        APIS --> RAW
    end

    subgraph TRANSFORM ["2. Transformação"]
        direction TB
        DBT["dbt"]
        STG["staging"]
        INT["intermediate"]
        MART["mart"]
        DBT --> STG --> INT --> MART
    end

    subgraph ENRICH ["3. Enriquecimento"]
        direction TB
        PROC["Postprocessors Python"]
        BQINT[("BigQuery intermediate")]
        PROC -->|MERGE SQL| BQINT
    end

    subgraph SCORE ["4. Score"]
        direction LR
        ENG["Engine v7.0"]
        CSV["CSV final"]
    end

    subgraph PUBLISH ["5. Publicação"]
        direction LR
        SNAP[("BigQuery snapshots")]
        SB[("Supabase")]
        WEB["Aplicação SolveLicita"]
    end

    PL --> INGEST
    RAW --> TRANSFORM
    MART --> ENRICH
    MART --> ENG
    BQINT --> ENG
    ENG --> SNAP
    ENG --> CSV
    CSV --> SB
    SB -. consumo .-> WEB

    style PL fill:#0f2744,color:#e8f3ff,stroke:#7fc4ff,stroke-width:2px
    style INGEST fill:#12263d,color:#e8f3ff,stroke:#185FA5,stroke-width:2px
    style TRANSFORM fill:#12263d,color:#e8f3ff,stroke:#2B7BBB,stroke-width:2px
    style ENRICH fill:#12263d,color:#e8f3ff,stroke:#4A97D9,stroke-width:2px
    style SCORE fill:#12263d,color:#e8f3ff,stroke:#6FB4F0,stroke-width:2px
    style PUBLISH fill:#12263d,color:#e8f3ff,stroke:#91CBFF,stroke-width:2px

    style APIS fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style RAW fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style DBT fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style STG fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style INT fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style MART fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style PROC fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style BQINT fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style ENG fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style CSV fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style SNAP fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style SB fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
    style WEB fill:#1a1f26,color:#f2f7ff,stroke:#8aa7c4,stroke-width:1px
```

Cada camada tem uma responsabilidade distinta:

- `raw`: dados oficiais coletados sem interpretação analítica.
- `staging`, `intermediate` e `mart`: modelagem e consolidação com `dbt`.
- `processors`: regras complementares em Python.
- `engine`: cálculo final do score e classificação de risco.
- `Supabase`: camada de dados consumida pela aplicação SolveLicita.

---

## Estrutura do repositório

```text
solvelicita-engine/
├── dbt/                    # Transformação SQL no data warehouse
│   ├── models/             # Camadas staging, intermediate e mart
│   └── dbt_project.yml     # Configuração do projeto dbt
├── docs/                   # Metodologia, validação e assets
├── src/                    # Motor Python de coleta, processamento e score
│   ├── collectors/         # Clientes de APIs públicas
│   ├── processors/         # Pós-processamento analítico
│   ├── engine/             # Cálculo final do score
│   ├── scorers/            # Curvas, pesos e pontuações parciais
│   ├── jobs/               # Orquestração testável do pipeline
│   └── analysis/           # Backtests e análises estatísticas
├── tests/                  # Testes automatizados
└── pipeline.py             # CLI principal do pipeline
```

---

## Reprodutibilidade

O pipeline pode ser reproduzido com dois ambientes isolados: um para coleta, processamento e score em Python; outro para transformação SQL com `dbt`.

```bash
git clone https://github.com/Fel-tby/solvelicita-engine.git
cd solvelicita-engine

# Ambiente principal
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
deactivate

# Ambiente dbt
python -m venv venv_dbt
venv_dbt\Scripts\activate
pip install dbt-core dbt-bigquery
deactivate
```

Configure as credenciais de Google Cloud e Supabase no arquivo `.env`, usando [`.env.example`](.env.example) como referência. Configure também o perfil dbt a partir de [`dbt/profiles.yml.example`](dbt/profiles.yml.example).

Execução principal:

```bash
venv\Scripts\activate
python pipeline.py --uf PB --mode incremental --steps collect,dbt,process,score,sync
```

Execução parcial, quando os dados já foram coletados e transformados:

```bash
python pipeline.py --uf PB --steps process,score,sync
```

---

## Documentação e Testes

| Documento | Conteúdo |
|---|---|
| [`docs/METODOLOGIA.md`](docs/METODOLOGIA.md) | Fórmula, pesos, curvas de pontuação, caps duros |
| [`docs/VALIDACAO.md`](docs/VALIDACAO.md) | Backtest, AUC-ROC, análise de sensibilidade |

**Executando Testes:**
```bash
# Camada SQL / BigQuery
venv_dbt\Scripts\activate
cd dbt && dbt test

# Camada Python / Score
venv\Scripts\activate
pytest -v
```

## GitHub Actions

O repositório possui automações em GitHub Actions:

- `CI`: roda `pytest` em `push` e `pull_request`.
- `Pipeline Nacional Full`: dispara a pipeline em matrix para as 27 UFs por `workflow_dispatch`.
- `Pipeline Incremental Mensal Nacional`: atualização incremental mensal para todas as UFs.
- `Pipeline CAUC Semanal Nacional`: atualização semanal de CAUC para todas as UFs.

Os workflows de pipeline reutilizam um executor único por UF e dependem dos secrets:

- `GCP_PROJECT_ID`
- `GCP_SA_KEY`
- `SUPABASE_URL`
- `SUPABASE_KEY`
