# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer — Tratamento, Deduplicação e Integridade
# MAGIC **Pipeline:** RPE Sales Data | **Camada:** Silver (Curated)

# COMMAND ----------
# CELL 1 — Imports e Configurações
import os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType, TimestampType, StringType
from pyspark.sql.window import Window

SOURCE_PATH = "/Volumes/workspace/default/rpe_landing/"
CATALOG     = "workspace"

print("✅ Configurações OK")

# COMMAND ----------
# CELL 2 — Leitura da camada Bronze
bronze_df = spark.table(f"{CATALOG}.default.bronze_sales_raw")
print(f"📊 Registros Bronze (com duplicatas): {bronze_df.count()}")

# COMMAND ----------
# CELL 3 — Tipagem e padronização
typed_df = (
    bronze_df
    .withColumn("order_id",   F.col("order_id").cast(StringType()))
    .withColumn("product_id", F.col("product_id").cast(IntegerType()))
    .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
    .withColumn("amount",     F.col("amount").cast(DoubleType()))
    .withColumn("status",     F.lower(F.trim(F.col("status"))))
    .withColumn("sale_date",  F.to_date(F.col("sale_date"), "yyyy-MM-dd"))
    .withColumn("discount",   F.col("discount").cast(DoubleType()))
    .withColumn("seller_id",  F.col("_seller_id"))
    .withColumn("ref_year",   F.col("_ref_year"))
    .withColumn("ref_month",  F.col("_ref_month"))
    .withColumn("source_file",F.col("_source_file"))
)

print("✅ Tipagem OK")

# COMMAND ----------
# CELL 4 — Filtros de qualidade
VALID_STATUSES = ["completed", "cancelled", "pending", "refunded"]

clean_df = (
    typed_df
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("seller_id").isNotNull())
    .filter(F.col("sale_date").isNotNull())
    .filter(F.col("amount") > 0)
    .filter(F.col("quantity") > 0)
    .filter(F.col("status").isin(VALID_STATUSES))
)

rejected_count = typed_df.count() - clean_df.count()
print(f"✅ Qualidade OK")
print(f"⚠️  Registros rejeitados por qualidade: {rejected_count}")

# COMMAND ----------
# CELL 5 — Deduplicação
dedup_window = Window.partitionBy(
    "order_id", "product_id", "quantity", "amount", "status", "sale_date", "seller_id"
).orderBy(F.col("discount").desc().nullsLast(), F.col("source_file").asc())

deduped_df = (
    clean_df
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

removed_dups = clean_df.count() - deduped_df.count()
print(f"✅ Deduplicação OK")
print(f"🗑️  Duplicatas removidas: {removed_dups}")
print(f"📊 Registros após dedup: {deduped_df.count()}")

# COMMAND ----------
# CELL 6 — Leitura das dimensões e integridade referencial
dim_seller = (
    spark.read
    .option("header", "true")
    .csv(f"{SOURCE_PATH}dim_seller.csv")
    .withColumn("seller_id", F.col("seller_id").cast(IntegerType()))
    .withColumn("seller_name", F.initcap(F.trim(F.col("seller_name"))))
    .withColumn("state", F.upper(F.trim(F.col("state"))))
)

dim_product = (
    spark.read
    .option("header", "true")
    .csv(f"{SOURCE_PATH}dim_product.csv")
    .withColumn("product_id", F.col("product_id").cast(IntegerType()))
    .withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
)

valid_seller_ids  = [r.seller_id  for r in dim_seller.collect()]
valid_product_ids = [r.product_id for r in dim_product.collect()]

enriched_df = (
    deduped_df
    .withColumn("seller_registered",  F.col("seller_id").isin(valid_seller_ids))
    .withColumn("product_registered", F.col("product_id").isin(valid_product_ids))
)

print("✅ Integridade referencial OK")
print("Sellers sem cadastro:")
enriched_df.filter(~F.col("seller_registered")).select("seller_id").distinct().show()
print("Produtos sem cadastro:")
enriched_df.filter(~F.col("product_registered")).select("product_id").distinct().show()

# COMMAND ----------
# CELL 7 — Late Arriving Data
enriched_df = enriched_df.withColumn(
    "late_arriving",
    (F.year(F.col("sale_date"))  != F.col("ref_year")) |
    (F.month(F.col("sale_date")) != F.col("ref_month"))
)

late_count = enriched_df.filter(F.col("late_arriving")).count()
print(f"✅ Late arriving data: {late_count} registros identificados")

# COMMAND ----------
# CELL 8 — Seleção das colunas finais
silver_sales_df = enriched_df.select(
    "order_id",
    "seller_id",
    "product_id",
    "sale_date",
    "quantity",
    "amount",
    "discount",
    "status",
    "ref_year",
    "ref_month",
    "seller_registered",
    "product_registered",
    "late_arriving",
    "source_file",
    F.lit(datetime.utcnow().isoformat()).cast(TimestampType()).alias("silver_ts"),
)

print(f"📊 Registros Silver prontos: {silver_sales_df.count()}")

# COMMAND ----------
# CELL 9 — Persistência Silver (saveAsTable)
silver_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{CATALOG}.default.silver_fact_sales")

print("✅ silver_fact_sales salva!")

# COMMAND ----------
# CELL 10 — Persistência dimensões Silver
dim_seller.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.default.silver_dim_sellers")

dim_product.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.default.silver_dim_products")

print("✅ silver_dim_sellers salva!")
print("✅ silver_dim_products salva!")

# COMMAND ----------
# CELL 11 — Preview final
print("📋 Preview Silver — Fato Vendas:")
spark.sql(f"""
    SELECT
        seller_id,
        ref_year,
        ref_month,
        COUNT(*) AS total_rows,
        ROUND(SUM(amount), 2) AS total_revenue,
        SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled
    FROM {CATALOG}.default.silver_fact_sales
    GROUP BY 1,2,3
    ORDER BY 1,2,3
""").show(50, truncate=False)

print("🥈 Silver layer concluída com sucesso!")
