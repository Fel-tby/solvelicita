# Metodologia do Score de Solvência

**Versão:** 2.0  
**Última atualização:** Fevereiro/2026  
**Aviso:** Score baseado exclusivamente em dados oficiais declarados pelo próprio município ao Tesouro Nacional (SICONFI) e ao CNJ (DataJud). Qualquer questionamento sobre os dados deve ser direcionado às fontes originais.

---

## Fórmula

    S = 25·Eorcam  +  20·(1 - Rrestos)  +  15·Qsiconfi
      + 20·(1 - Ccauc)  +  10·(1 - Jdatajud)
      +  7·(1 - Atcu)   +   3·(1 - Sceis)

## Variáveis

| Variável    | Fonte          | O que mede                                             | Peso |
|-------------|----------------|--------------------------------------------------------|------|
| `Eorcam`    | SICONFI        | Execução orçamentária média 2020–2024                  | 25%  |
| `Rrestos`   | SICONFI        | Proporção de restos a pagar não processados            | 20%  |
| `Qsiconfi`  | Ranking STN    | Qualidade e pontualidade dos envios ao SICONFI         | 15%  |
| `Ccauc`     | CAUC/STN       | Frequência de bloqueios para receber repasses federais | 20%  |
| `Jdatajud`  | DataJud/CNJ    | Ações judiciais de fornecedores por inadimplência      | 10%  |
| `Atcu`      | TCU            | Achados críticos de auditoria nos últimos 3 anos       |  7%  |
| `Sceis`     | CEIS/CNEP      | Órgão ou gestor com sanções ativas                     |  3%  |

## Classificação

| Score  | Classificação      | Significado operacional                    |
|--------|--------------------|--------------------------------------------|
| 80–100 | 🟢 Risco Baixo     | Histórico consistente de pagamento         |
| 60–79  | 🟡 Risco Médio     | Avaliar antes de participar                |
| 40–59  | 🔴 Risco Alto      | Exigir garantias ou evitar                 |
| 0–39   | ⛔ Crítico         | Histórico grave de inadimplência           |

## Tratamento de dados ausentes

| Situação                        | Comportamento                                                 |
|---------------------------------|---------------------------------------------------------------|
| Município sem dados SICONFI     | Score parcial com aviso: "Dados fiscais indisponíveis"        |
| Município sem histórico no PNCP | Score calculado, rotulado "sem histórico de compras recentes" |
| Dados com mais de 18 meses      | Aviso de desatualização visível                               |

## Limitações

- Dados SICONFI podem ser enviados com atraso por municípios pequenos
- Score não captura acordos informais de pagamento
- Dados declarados pelo próprio município podem estar incompletos
- Não substitui due diligence jurídica para contratos de alto valor
