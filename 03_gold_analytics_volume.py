# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer — Modelagem Analítica (Star Schema)
# MAGIC **Pipeline:** RPE Sales Data | **Camada:** Gold (Analytics-Ready)

# COMMAND ----------
# CELL 1 — Imports e Configurações
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType, StringType
from datetime import datetime, date
import pandas as pd

CATALOG = "workspace"

print("✅ Configurações OK")

# COMMAND ----------
# CELL 2 — Leitura das tabelas Silver
fact_sales   = spark.table(f"{CATALOG}.default.silver_fact_sales")
dim_sellers  = spark.table(f"{CATALOG}.default.silver_dim_sellers")
dim_products = spark.table(f"{CATALOG}.default.silver_dim_products")

completed = fact_sales.filter(F.col("status") == "completed")

print(f"📊 Fato vendas total    : {fact_sales.count()}")
print(f"📊 Fato vendas completed: {completed.count()}")

# COMMAND ----------
# CELL 3 — Dim Date
dates = pd.date_range("2025-01-01", "2025-12-31", freq="D")
dim_date_pd = pd.DataFrame({
    "date_id":    dates.strftime("%Y%m%d").astype(int),
    "date":       dates.astype(str),
    "year":       dates.year,
    "month":      dates.month,
    "month_name": dates.strftime("%B"),
    "quarter":    dates.quarter,
    "day_of_week":dates.day_name(),
})

dim_date = spark.createDataFrame(dim_date_pd)
dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.gold_dim_date")
print("✅ gold_dim_date criada!")

# COMMAND ----------
# CELL 4 — Dim Seller Gold
dim_seller_gold = (
    dim_sellers
    .withColumn("is_unregistered", F.lit(False))
)

unregistered_sellers = (
    fact_sales
    .filter(~F.col("seller_registered"))
    .select("seller_id").distinct()
    .withColumn("seller_name",     F.lit("Unknown"))
    .withColumn("state",           F.lit(None).cast(StringType()))
    .withColumn("is_unregistered", F.lit(True))
)

dim_seller_gold_final = dim_seller_gold.unionByName(
    unregistered_sellers, allowMissingColumns=True
)

dim_seller_gold_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.default.gold_dim_seller")

print("✅ gold_dim_seller criada!")

# COMMAND ----------
# CELL 5 — Dim Product Gold
dim_product_gold = dim_products.withColumn("is_unregistered", F.lit(False))

unregistered_products = (
    fact_sales
    .filter(~F.col("product_registered"))
    .select("product_id").distinct()
    .withColumn("product_name",    F.lit("Unknown"))
    .withColumn("category",        F.lit("Unknown"))
    .withColumn("is_unregistered", F.lit(True))
)

dim_product_gold_final = dim_product_gold.unionByName(
    unregistered_products, allowMissingColumns=True
)

dim_product_gold_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.default.gold_dim_product")

print("✅ gold_dim_product criada!")

# COMMAND ----------
# CELL 6 — Fato Vendas Gold enriquecida
fact_gold = (
    fact_sales
    .join(dim_sellers.select("seller_id","state"), on="seller_id", how="left")
    .join(dim_products.select("product_id","product_name","category"), on="product_id", how="left")
    .withColumn("net_amount", F.col("amount") - F.coalesce(F.col("discount"), F.lit(0.0)))
    .withColumn("year_month", F.date_format(F.col("sale_date"), "yyyy-MM"))
    .withColumn("gold_ts",    F.lit(datetime.utcnow().isoformat()).cast(TimestampType()))
)

fact_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.default.gold_fact_sales")

print(f"✅ gold_fact_sales criada: {fact_gold.count()} registros!")

# COMMAND ----------
# MAGIC %md ## Perguntas Analíticas

# COMMAND ----------
# CELL 7 — Receita total por mês
print("📊 1. RECEITA TOTAL POR MÊS:")
spark.sql(f"""
    SELECT ref_year, ref_month,
           ROUND(SUM(amount), 2) AS total_revenue,
           COUNT(order_id) AS total_orders
    FROM {CATALOG}.default.gold_fact_sales
    WHERE status = 'completed'
    GROUP BY 1,2 ORDER BY 1,2
""").write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.gold_agg_monthly_revenue")
spark.table(f"{CATALOG}.default.gold_agg_monthly_revenue").show(20, truncate=False)

# COMMAND ----------
# CELL 8 — Ticket médio
print("📊 2. TICKET MÉDIO POR PEDIDO:")
spark.sql(f"""
    SELECT ROUND(AVG(amount), 2) AS avg_ticket
    FROM {CATALOG}.default.gold_fact_sales
    WHERE status = 'completed'
""").show()

# COMMAND ----------
# CELL 9 — Top 5 produtos por receita
print("📊 3. TOP 5 PRODUTOS POR RECEITA:")
spark.sql(f"""
    SELECT product_id, product_name,
           ROUND(SUM(amount), 2) AS total_revenue
    FROM {CATALOG}.default.gold_fact_sales
    WHERE status = 'completed'
    GROUP BY 1,2 ORDER BY 3 DESC LIMIT 5
""").write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.gold_top5_products_revenue")
spark.table(f"{CATALOG}.default.gold_top5_products_revenue").show(truncate=False)

# COMMAND ----------
# CELL 10 — Top 5 produtos por quantidade
print("📊 4. TOP 5 PRODUTOS POR QUANTIDADE:")
spark.sql(f"""
    SELECT product_id, product_name,
           SUM(quantity) AS total_quantity
    FROM {CATALOG}.default.gold_fact_sales
    WHERE status = 'completed'
    GROUP BY 1,2 ORDER BY 3 DESC LIMIT 5
""").write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.gold_top5_products_qty")
spark.table(f"{CATALOG}.default.gold_top5_products_qty").show(truncate=False)

# COMMAND ----------
# CELL 11 — Top 5 vendedores por receita
print("📊 5. TOP 5 VENDEDORES POR RECEITA:")
spark.sql(f"""
    SELECT f.seller_id, s.seller_name,
           ROUND(SUM(f.amount), 2) AS total_revenue
    FROM {CATALOG}.default.gold_fact_sales f
    LEFT JOIN {CATALOG}.default.gold_dim_seller s ON f.seller_id = s.seller_id
    WHERE f.status = 'completed'
    GROUP BY 1,2 ORDER BY 3 DESC LIMIT 5
""").write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.gold_top5_sellers")
spark.table(f"{CATALOG}.default.gold_top5_sellers").show(truncate=False)

# COMMAND ----------
# CELL 12 — Recorrentes vs Novos
print("📊 6. VENDEDORES RECORRENTES VS NOVOS:")
spark.sql(f"""
    SELECT
        CASE WHEN active_months > 1 THEN 'Recorrente' ELSE 'Novo' END AS tipo,
        COUNT(*) AS quantidade
    FROM (
        SELECT seller_id,
               COUNT(DISTINCT CONCAT(ref_year,'-',ref_month)) AS active_months
        FROM {CATALOG}.default.gold_fact_sales
        GROUP BY seller_id
    )
    GROUP BY 1
""").show()

# COMMAND ----------
# CELL 13 — % Cancelados
print("📊 7. PERCENTUAL DE PEDIDOS CANCELADOS:")
spark.sql(f"""
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelados,
        ROUND(SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_cancelados
    FROM {CATALOG}.default.gold_fact_sales
""").show()

# COMMAND ----------
# CELL 14 — Faturamento por estado
print("📊 8. FATURAMENTO POR ESTADO:")
spark.sql(f"""
    SELECT state,
           ROUND(SUM(amount), 2) AS total_revenue
    FROM {CATALOG}.default.gold_fact_sales
    WHERE status = 'completed'
    GROUP BY 1 ORDER BY 2 DESC
""").write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.gold_revenue_by_state")
spark.table(f"{CATALOG}.default.gold_revenue_by_state").show(truncate=False)

# COMMAND ----------
# CELL 15 — Vendedores inativos
print("📊 9. VENDEDORES INATIVOS (>30 dias):")
spark.sql(f"""
    SELECT f.seller_id, s.seller_name,
           MAX(f.sale_date) AS last_sale,
           DATEDIFF(CURRENT_DATE(), MAX(f.sale_date)) AS days_inactive
    FROM {CATALOG}.default.gold_fact_sales f
    LEFT JOIN {CATALOG}.default.gold_dim_seller s ON f.seller_id = s.seller_id
    WHERE f.status = 'completed'
    GROUP BY 1,2
    HAVING DATEDIFF(CURRENT_DATE(), MAX(f.sale_date)) > 30
    ORDER BY 4 DESC
""").write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.gold_inactive_sellers")
spark.table(f"{CATALOG}.default.gold_inactive_sellers").show(truncate=False)

# COMMAND ----------
# CELL 16 — MoM por vendedor
print("📊 10. VARIAÇÃO MoM POR VENDEDOR:")
spark.sql(f"""
    SELECT seller_id, ref_year, ref_month, revenue, prev_revenue,
           ROUND((revenue - prev_revenue) / prev_revenue * 100, 2) AS mom_pct
    FROM (
        SELECT seller_id, ref_year, ref_month,
               ROUND(SUM(amount), 2) AS revenue,
               LAG(ROUND(SUM(amount), 2)) OVER (
                   PARTITION BY seller_id ORDER BY ref_year, ref_month
               ) AS prev_revenue
        FROM {CATALOG}.default.gold_fact_sales
        WHERE status = 'completed'
        GROUP BY 1,2,3
    )
    WHERE prev_revenue IS NOT NULL
    ORDER BY seller_id, ref_year, ref_month
""").write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.gold_seller_mom")
spark.table(f"{CATALOG}.default.gold_seller_mom").show(50, truncate=False)

# COMMAND ----------
# CELL 17 — Queda 3 meses consecutivos
print("📊 11. VENDEDORES COM QUEDA 3 MESES CONSECUTIVOS:")
spark.sql(f"""
    SELECT seller_id, ref_year, ref_month, revenue, prev1, prev2
    FROM (
        SELECT seller_id, ref_year, ref_month,
               ROUND(SUM(amount),2) AS revenue,
               LAG(ROUND(SUM(amount),2),1) OVER (PARTITION BY seller_id ORDER BY ref_year, ref_month) AS prev1,
               LAG(ROUND(SUM(amount),2),2) OVER (PARTITION BY seller_id ORDER BY ref_year, ref_month) AS prev2
        FROM {CATALOG}.default.gold_fact_sales
        WHERE status = 'completed'
        GROUP BY 1,2,3
    )
    WHERE revenue < prev1 AND prev1 < prev2
    ORDER BY seller_id, ref_year, ref_month
""").write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.gold_consecutive_drops")
spark.table(f"{CATALOG}.default.gold_consecutive_drops").show(truncate=False)

# COMMAND ----------
print("🥇 Gold layer concluída com sucesso!")
print(f"\nTabelas Gold criadas:")
spark.sql(f"SHOW TABLES IN {CATALOG}.default").filter("tableName LIKE 'gold%'").show(truncate=False)
