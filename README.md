# RPE Sales Pipeline

Pipeline de dados de vendas desenvolvido como parte do processo seletivo da RPE.

**Autora:** Laura Virginia Ferreira Soares  
**Stack:** Databricks · PySpark · Delta Lake · Python  
**Ambiente:** Databricks Free Edition 

---

## Sobre o projeto

Recebi múltiplos arquivos CSV com vendas mensais de vendedores e precisei construir um pipeline completo do zero. O identificador do vendedor não estava dentro do arquivo precisei extrair o `seller_id`, ano e mês diretamente do nome do arquivo via regex.

O dataset veio com vários cenários propositais que precisaram ser tratados: dados duplicados, vendedores e produtos sem cadastro, arquivos com nome inválido, schema diferente entre arquivos e dados fora do período de referência (late arriving data).

Optei pelo **Databricks com Delta Lake** por ser o ambiente mais próximo do que a RPE utiliza no dia a dia.

---

## Estrutura do pipeline

```
landing/                          ← arquivos CSV originais
├── 1_2025_01_sales.csv
├── 5_2025_11_sales.csv
├── dim_seller.csv
├── dim_product.csv
└── ...

notebooks/
├── 01_bronze_ingestion_volume.py
├── 02_silver_transformation_volume.py
├── 03_gold_analytics_volume.py
└── 04_quality_monitor_volume.py
```

---

## Camadas do pipeline

### Bronze — ingestão raw

Primeiro passo: ler os arquivos e validar o nome antes de qualquer coisa. Usei regex para extrair `seller_id`, ano e mês do filename. Arquivos com nome inválido (como `abc_2025_99_sales.csv`) ou schema errado são rejeitados e salvos em uma tabela de rejeição  nunca descartados.

Implementei um controle de idempotência via tabela `_control/` para que o mesmo arquivo nunca seja ingerido duas vezes, mesmo que o job rode mais de uma vez.

**Cenários tratados:**
- Nome de arquivo inválido → rejeitado com motivo registrado
- Schema evolution entre arquivos → `mergeSchema = true`
- Todos os campos lidos como string → tipagem é responsabilidade da Silver

### Silver — limpeza e qualidade

Aqui é onde a maioria dos problemas apareceu. Fiz tipagem correta de todos os campos, padronização de strings (trim, lower, initcap), deduplicação com lógica de negócio (em caso de duplicata, mantém o registro com maior desconto) e validação de qualidade (sem amount negativo, sem status inválido, sem datas nulas).

Vendedores e produtos sem cadastro **não são descartados** preferi sinalizar com flags (`seller_registered`, `product_registered`) para não perder informação de negócio. O mesmo para dados fora do período de referência (`late_arriving`).

Usei MERGE upsert por chave de negócio `(order_id, product_id, seller_id)` para garantir exactly-once no reprocessamento.

**Cenários tratados:**
- 144 duplicatas removidas
- Seller 3 sem cadastro → sinalizado, não descartado
- Produtos 10 e 25 sem cadastro → mesmo tratamento
- 2 arquivos inválidos → rejeitados na Bronze

### Gold — modelagem analítica

Modelei em **Star Schema** com uma tabela fato central e quatro dimensões. Respondi todas as 11 perguntas analíticas do case com tabelas `agg_*` materializadas.

**Por que Star Schema?**

As dimensões deste projeto são pequenas (menos de 50 sellers, menos de 50 produtos), então normalizar mais (Snowflake) não agrega valor. A Wide Table sacrifica flexibilidade analítica. O Star Schema é o equilíbrio ideal joins de um único nível, ideal para Spark e Delta Lake, com particionamento na fato e Z-order para acelerar queries. Alem disso esse modelo perfoma melhor para agregação analitica e trona as consultas menos complexas, falicita o uso para ferramentas de BI.

**Modelo:**
```
              dim_seller
                  │
dim_date ── fact_sales ── dim_product
```

### Quality Monitor — observabilidade

Adicionei checagens automáticas no final do pipeline: volume mínimo de registros, taxa de cancelamento, ausência de valores nulos, cobertura de sellers cadastrados e range de datas válido. Os resultados ficam registrados na tabela `pipeline_quality_log` e o job aborta com exceção explícita se alguma checagem crítica falhar. adicionando assim estrategias de monitoramentos ultilizando metricas de volume de ingestão, arquivos reijeitados, percentual de duplicidade, duração das execuçoes e façhas por etapas.

---

## Orquestração

Criei um Job no Databricks com 4 tasks em sequência, cada uma dependendo do sucesso da anterior:

```
01_BRONZE → 02_SILVER → 03_GOLD → 04_QUALITY_MONITOR
```

Configurações de resiliência:
- Bronze: retry 3x com intervalo de 1 minuto
- Silver: retry 3x com intervalo de 1 minuto
- Gold: retry 2x com intervalo de 2 minutos
- Quality Monitor: falha explícita sem retry (para alertar, não silenciar)

---

## Resiliência e Observabilidade

**Como garanto reexecução segura:**
- Bronze: tabela `_control/` impede reingestão do mesmo arquivo
- Silver: MERGE por chave de negócio — reprocessar os mesmos dados resulta em UPDATE, não duplicata
- Gold: `mode("overwrite")` nas agregações — sempre recalculado a partir do Silver

**Como evito perda de dados:**
- Delta Lake time travel: possível restaurar qualquer versão anterior
- Dados rejeitados nunca são deletados — ficam em tabelas de auditoria
- Landing zone é read-only para o pipeline

**Como garanto exactly-once:**
- Bronze: ledger `_control/` + Delta ACID (commit atômico por arquivo)
- Silver: MERGE idempotente por definição
- Gold: recalculo total a partir do Silver (determinístico)

---

## Perguntas analíticas respondidas

| # | Pergunta | Tabela Gold |
|---|---|---|
| 1 | Receita total por mês | `gold_agg_monthly_revenue` |
| 2 | Ticket médio por pedido | calculado inline |
| 3 | Top 5 produtos por receita | `gold_top5_products_revenue` |
| 4 | Top 5 produtos por quantidade | `gold_top5_products_qty` |
| 5 | Top 5 vendedores por receita | `gold_top5_sellers` |
| 6 | Vendedores recorrentes vs novos | calculado inline |
| 7 | % pedidos cancelados | calculado inline |
| 8 | Faturamento por estado | `gold_revenue_by_state` |
| 9 | Vendedores inativos >30 dias | `gold_inactive_sellers` |
| 10 | Variação MoM por vendedor | `gold_seller_mom` |
| 11 | Queda 3 meses consecutivos | `gold_consecutive_drops` |

---

## O que encontrei nos dados

| Cenário | Quantidade | Tratamento |
|---|---|---|
| Duplicatas | 144 registros | Dedup — mantém maior desconto |
| Arquivos inválidos | 2 arquivos | Rejeitados com motivo registrado |
| Seller sem cadastro | Seller 3 | Sinalizado com flag |
| Produtos sem cadastro | Produtos 10 e 25 | Sinalizados com flag |
| Schema evolution | 1 arquivo sem coluna `discount` | `mergeSchema = true` |

---

## Conclusão

Este projeto demonstra a implementação de um pipeline analítico resiliente utilizando práticas modernas de Engenharia de Dados com foco em:

escalabilidade
manutenibilidade
observabilidade
reprocessamento seguro
performance analítica
governança operacional

A solução foi projetada considerando cenários corporativos reais e princípios arquiteturais orientados a produção.
