# SolveLicita — Dashboard

Dashboard interativo de capacidade de pagamento dos municípios da Paraíba, construído em Next.js e publicado em tempo real a partir dos dados do pipeline SolveLicita.

Os dados são atualizados a cada execução do pipeline e armazenados no Supabase. O frontend os consulta diretamente — sem build necessário para refletir novos scores.

## O que mostra

- **Mapa coroplético** dos 223 municípios classificados por faixa de risco de solvência
- **Ranking completo** com ordenação por qualquer indicador: score, execução orçamentária, CAUC, Lliq, autonomia tributária, % de dispensa e outros
- **Alertas por município**: dispensa acima de 30%, dado suspeito, autonomia crítica, RP crônico
- **Medianas estaduais** dos principais indicadores fiscais
- **Filtros** por classificação, faixa de score e nome do município