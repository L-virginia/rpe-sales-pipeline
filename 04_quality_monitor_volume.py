# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Quality Monitor — Checagens de Qualidade
# MAGIC **Pipeline:** RPE Sales Data | **Camada:** Observabilidade

# COMMAND ----------
# CELL 1 — Configurações
from datetime import datetime

CATALOG = "workspace"
MAX_CANCEL_PCT = 60.0
MIN_ROWS       = 100

print("✅ Configurações OK")

# COMMAND ----------
# CELL 2 — Checagens de qualidade
checks = []

def run_check(name, query, threshold, operator=">=", critical=True):
    try:
        result = spark.sql(query).collect()[0][0]
        passed = eval(f"{result} {operator} {threshold}")
        status = "✅ PASS" if passed else ("❌ FAIL CRITICAL" if critical else "⚠️ WARN")
        checks.append({
            "check": name, "result": str(result),
            "threshold": str(threshold), "status": status,
            "ts": datetime.utcnow().isoformat()
        })
        print(f"{status} | {name}: {result} {operator} {threshold}")
        return passed
    except Exception as e:
        checks.append({"check": name, "result": str(e), "status": "❌ ERROR", "ts": datetime.utcnow().isoformat()})
        print(f"❌ ERROR | {name}: {e}")
        return False

# COMMAND ----------
# CELL 3 — Executar checagens

# Volume mínimo de registros
run_check(
    "min_rows_gold_fact_sales",
    f"SELECT COUNT(*) FROM {CATALOG}.default.gold_fact_sales",
    threshold=MIN_ROWS
)

# Taxa de cancelamento
run_check(
    "cancel_rate_below_threshold",
    f"""
    SELECT ROUND(
        SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    ) FROM {CATALOG}.default.gold_fact_sales
    """,
    threshold=MAX_CANCEL_PCT,
    operator="<="
)

# Sem amount nulo ou negativo
run_check(
    "no_null_or_negative_amount",
    f"SELECT COUNT(*) FROM {CATALOG}.default.gold_fact_sales WHERE amount IS NULL OR amount <= 0",
    threshold=0,
    operator="=="
)

# Cobertura de sellers cadastrados
run_check(
    "registered_sellers_coverage_pct",
    f"""
    SELECT ROUND(
        SUM(CASE WHEN seller_registered THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    ) FROM {CATALOG}.default.gold_fact_sales
    """,
    threshold=70.0
)

# Datas dentro do range esperado
run_check(
    "sale_dates_in_valid_range",
    f"""
    SELECT COUNT(*) FROM {CATALOG}.default.gold_fact_sales
    WHERE sale_date < '2020-01-01' OR sale_date > '2030-12-31'
    """,
    threshold=0,
    operator="==",
    critical=False
)

# Dimensões não vazias
run_check(
    "dim_seller_not_empty",
    f"SELECT COUNT(*) FROM {CATALOG}.default.gold_dim_seller",
    threshold=1
)

run_check(
    "dim_product_not_empty",
    f"SELECT COUNT(*) FROM {CATALOG}.default.gold_dim_product",
    threshold=1
)

# COMMAND ----------
# CELL 4 — Salvar log de qualidade
quality_df = spark.createDataFrame(checks)
quality_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.default.pipeline_quality_log")

print("\n📋 Resultado das checagens:")
quality_df.show(truncate=False)

# COMMAND ----------
# CELL 5 — Métricas do pipeline
spark.sql(f"""
    SELECT
        '{datetime.utcnow().isoformat()}'                                          AS pipeline_run_ts,
        COUNT(*)                                                                    AS total_records,
        COUNT(DISTINCT seller_id)                                                   AS distinct_sellers,
        COUNT(DISTINCT product_id)                                                  AS distinct_products,
        MIN(sale_date)                                                              AS earliest_sale,
        MAX(sale_date)                                                              AS latest_sale,
        ROUND(SUM(CASE WHEN status='completed' THEN amount ELSE 0 END), 2)         AS total_revenue,
        ROUND(AVG(CASE WHEN status='completed' THEN amount END), 2)                AS avg_ticket,
        ROUND(SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancel_pct
    FROM {CATALOG}.default.gold_fact_sales
""").write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.default.pipeline_run_metrics")

print("📊 Métricas de pipeline salvas!")

# COMMAND ----------
# CELL 6 — Falha crítica se necessário
critical_failures = [c for c in checks if "FAIL CRITICAL" in c["status"] or "ERROR" in c["status"]]

if critical_failures:
    failed = [c["check"] for c in critical_failures]
    raise Exception(f"❌ PIPELINE ABORTADO — Checagens críticas falharam: {failed}")

print("✅ Todas as checagens críticas passaram!")
print("🔍 Quality Monitor concluído com sucesso!")
