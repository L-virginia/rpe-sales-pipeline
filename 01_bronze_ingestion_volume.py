# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer — Ingestão Raw
# MAGIC **Pipeline:** RPE Sales Data | **Camada:** Bronze

# COMMAND ----------
# CELL 1 — Imports e Configurações
import os
import re
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

SOURCE_PATH = "/Volumes/workspace/default/rpe_landing/"
BRONZE_PATH = "/Volumes/workspace/default/rpe_bronze/"
CATALOG     = "workspace"

print("✅ Configurações OK")
print(f"Source : {SOURCE_PATH}")
print(f"Bronze : {BRONZE_PATH}")

# COMMAND ----------
# CELL 2 — Funções auxiliares
REQUIRED_COLS = {"order_id", "product_id", "quantity", "amount", "status", "sale_date"}
FILE_PATTERN  = re.compile(r"^(\d+)_(\d{4})_(\d{2})_sales.*\.csv$")

def parse_filename(filename):
    """Extrai seller_id, year, month do nome do arquivo."""
    m = FILE_PATTERN.match(os.path.basename(filename))
    if not m:
        return None
    sid, yr, mo = m.groups()
    if not (1 <= int(mo) <= 12):
        return None
    return int(sid), int(yr), int(mo)

def has_required_columns(df):
    return REQUIRED_COLS.issubset(set(df.columns))

print("✅ Funções OK")

# COMMAND ----------
# CELL 3 — Descoberta dos arquivos CSV
all_files = [
    f"{SOURCE_PATH}{f}"
    for f in os.listdir(SOURCE_PATH)
    if f.endswith(".csv")
]

print(f"📂 Arquivos encontrados: {len(all_files)}")
for f in sorted(all_files):
    print(f"  {os.path.basename(f)}")

# COMMAND ----------
# CELL 4 — Validação e separação de arquivos válidos/inválidos
valid_files   = []
invalid_files = []

for filepath in all_files:
    filename = os.path.basename(filepath)
    parsed   = parse_filename(filename)

    if parsed is None:
        print(f"⚠️  REJEITADO (nome inválido): {filename}")
        invalid_files.append((filepath, "invalid_filename_pattern"))
        continue

    try:
        header_df = spark.read.option("header", "true").csv(filepath).limit(0)
        if not has_required_columns(header_df):
            print(f"⚠️  REJEITADO (colunas faltando): {filename}")
            invalid_files.append((filepath, "missing_required_columns"))
            continue
    except Exception as e:
        print(f"⚠️  REJEITADO (erro leitura): {filename} — {str(e)[:100]}")
        invalid_files.append((filepath, "read_error"))
        continue

    valid_files.append((filepath, *parsed))
    print(f"✅ VÁLIDO: {filename}")

print(f"\n📊 Válidos   : {len(valid_files)}")
print(f"📊 Rejeitados: {len(invalid_files)}")

# COMMAND ----------
# CELL 5 — Persistência dos arquivos rejeitados
if invalid_files:
    rejected_df = spark.createDataFrame(
        [(f, r, datetime.utcnow().isoformat()) for f, r in invalid_files],
        schema=["filename", "rejection_reason", "rejected_at"]
    )
    (
        rejected_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(f"{BRONZE_PATH}_rejected/")
    )
    print(f"⚠️  {len(invalid_files)} arquivos rejeitados salvos em _rejected/")

# COMMAND ----------
# CELL 6 — Controle de idempotência (arquivos já processados)
CONTROL_PATH = f"{BRONZE_PATH}_control/"

def get_processed_files():
    try:
        return set(
            spark.read.format("delta").load(CONTROL_PATH)
            .select("filename").rdd.flatMap(lambda x: x).collect()
        )
    except Exception:
        return set()

def mark_as_processed(filename, seller_id, year, month, rows):
    df = spark.createDataFrame(
        [(filename, seller_id, year, month, rows, datetime.utcnow().isoformat())],
        schema=["filename", "seller_id", "ref_year", "ref_month", "row_count", "processed_at"]
    )
    df.write.format("delta").mode("append").save(CONTROL_PATH)

already_processed = get_processed_files()
print(f"📋 Arquivos já processados anteriormente: {len(already_processed)}")

# COMMAND ----------
# CELL 7 — Ingestão e persistência Bronze
new_files_processed = 0

for filepath, seller_id, year, month in valid_files:
    filename = os.path.basename(filepath)

    if filename in already_processed:
        print(f"⏭️  SKIP (já processado): {filename}")
        continue

    print(f"📥 Processando: {filename}")

    try:
        raw_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(filepath)
        )

        enriched_df = (
            raw_df
            .withColumn("_seller_id",   F.lit(seller_id).cast(IntegerType()))
            .withColumn("_ref_year",    F.lit(year).cast(IntegerType()))
            .withColumn("_ref_month",   F.lit(month).cast(IntegerType()))
            .withColumn("_source_file", F.lit(filename))
            .withColumn("_bronze_ts",   F.lit(datetime.utcnow().isoformat()).cast(TimestampType()))
        )

        row_count = enriched_df.count()

        (
            enriched_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(BRONZE_PATH)
        )

        mark_as_processed(filename, seller_id, year, month, row_count)
        new_files_processed += 1
        print(f"   ✅ {row_count} linhas gravadas")

    except Exception as e:
        print(f"   ❌ Erro: {e}")

print(f"\n📊 Novos arquivos processados: {new_files_processed}")

# COMMAND ----------
# CELL 8 — Registrar tabela no Metastore e exibir resultado
spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze_sales_raw
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

print("📋 Preview da tabela Bronze:")
spark.sql(f"""
    SELECT
        _seller_id,
        _ref_year,
        _ref_month,
        COUNT(*) AS total_rows
    FROM {CATALOG}.bronze_sales_raw
    GROUP BY 1,2,3
    ORDER BY 1,2,3
""").show(50, truncate=False)

print("🥉 Bronze layer concluída com sucesso!")
