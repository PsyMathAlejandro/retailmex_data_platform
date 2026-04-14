"""
silver_transform.py
===================
Job PySpark — Capa Silver / Trusted
RetailMex Data Platform

Responsabilidad:
    Leer datos crudos de la capa Bronze (Parquet en S3), aplicar
    limpieza, validación, deduplicación y estandarización de esquema,
    y escribir el resultado en la capa Silver particionado por fecha.

Fuentes consolidadas:
    - Ventas tiendas físicas  (bronze/ventas_tiendas/)
    - Ventas e-commerce       (bronze/ventas_ecommerce/)
    - Inventario ERP          (bronze/inventario/)

Ejecución:
    Llamado por AWS Step Functions después del job Bronze.
    Corre cada 15 minutos vía EventBridge Scheduler.
"""

import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Inicialización del contexto Glue / Spark
# ---------------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bronze_bucket",   # ej: s3://retailmex-datalake/bronze
        "silver_bucket",   # ej: s3://retailmex-datalake/silver
        "run_date",        
        "run_minute",      
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE = args["bronze_bucket"]
SILVER = args["silver_bucket"]
RUN_DATE = args["run_date"]          # "2025-04-13"
RUN_TS = datetime.now(timezone.utc)  # timestamp real de ejecución

# ---------------------------------------------------------------------------
# Esquema de ventas Silver
# Todas las fuentes.
# ---------------------------------------------------------------------------

SCHEMA_VENTAS_SILVER = StructType([
    StructField("venta_id",StringType(),   nullable=False), 
    StructField("canal",StringType(),   nullable=False),  # 'tienda_fisica' | 'ecommerce'
    StructField("tienda_id",StringType(),   nullable=True), # null si es ecommerce puro
    StructField("tienda_nombre",StringType(),   nullable=True),
    StructField("sku",StringType(),   nullable=False),
    StructField("cantidad",IntegerType(),  nullable=False),
    StructField("precio_unitario", DecimalType(10, 2), nullable=False),
    StructField("monto_total",DecimalType(12, 2), nullable=False),
    StructField("moneda",StringType(),   nullable=False),  # 'MXN'
    StructField("vendido_en",TimestampType(), nullable=False), # UTC
    StructField("cliente_id",StringType(),   nullable=True),  
    StructField("_fuente",StringType(),   nullable=False),  # tabla Bronze de origen
    StructField("_procesado_en",   TimestampType(), nullable=False), # timestamp de este job
])

# ---------------------------------------------------------------------------
# Esquema inventario Silver
# ---------------------------------------------------------------------------

SCHEMA_INVENTARIO_SILVER = StructType([
    StructField("inventario_id",   StringType(),   nullable=False),
    StructField("tienda_id",       StringType(),   nullable=False),
    StructField("tienda_nombre",   StringType(),   nullable=True),
    StructField("sku",             StringType(),   nullable=False),
    StructField("stock_disponible", IntegerType(), nullable=False),
    StructField("stock_reservado",  IntegerType(), nullable=False),
    StructField("ultima_actualizacion", TimestampType(), nullable=False),
    StructField("_fuente",         StringType(),   nullable=False),
    StructField("_procesado_en",   TimestampType(), nullable=False),
])


# ---------------------------------------------------------------------------
# calidad de datos
# ---------------------------------------------------------------------------

def log_quality(df: DataFrame, stage: str, tabla: str) -> None:
    """
    Imprime métricas básicas de calidad en CloudWatch Logs.
    Step Functions captura estos logs para alertas operativas.
    """
    total = df.count()
    nulos_sku = df.filter(F.col("sku").isNull()).count()
    nulos_monto = df.filter(F.col("monto_total").isNull() if "monto_total" in df.columns else F.lit(False)).count()
    print(f"[QUALITY] {tabla} | stage={stage} | total={total} | nulos_sku={nulos_sku} | nulos_monto={nulos_monto}")


def mask_pii(df: DataFrame, columna: str) -> DataFrame:
    """
    Enmascara datos PII aplicando SHA-256 a la columna indicada.
    También podemos agregar datos de tarjetas.
    """
    return df.withColumn(columna, F.sha2(F.col(columna).cast(StringType()), 256))


def validar_no_vacios(df: DataFrame, columnas: list) -> DataFrame:
    condicion = F.lit(True)
    for col in columnas:
        condicion = condicion & F.col(col).isNotNull() & (F.trim(F.col(col)) != "")
    rechazados = df.filter(~condicion)
    if rechazados.count() > 0:
        print(f"[WARN] {rechazados.count()} registros rechazados por nulos en {columnas}")
    return df.filter(condicion)


def deduplicar(df: DataFrame, llave: str, orden: str) -> DataFrame:
    """
    Elimina duplicados conservando el registro más reciente.
    """
    from pyspark.sql.window import Window
    ventana = Window.partitionBy(llave).orderBy(F.col(orden).desc())
    
    #particionamos por la llave, y si existe más de dos veces, se queda con el primer registro encontrado
    return (
        df.withColumn("duplicado", F.row_number().over(ventana))
          .filter(F.col("dublicado") == 1)
          .drop("duplicado")
    )


# ---------------------------------------------------------------------------
# Transformación: Ventas tiendas físicas (Bronze → Silver)
# ---------------------------------------------------------------------------

def transformar_ventas_tiendas(bronze_path: str) -> DataFrame:
    """
    Lee ventas del POS de tiendas físicas desde Bronze y las normaliza
    al esquema canónico Silver.

    Reglas de negocio aplicadas:
    - monto_total debe ser positivo (filtra devoluciones — van a tabla aparte)
    - sku se estandariza a mayúsculas sin espacios
    - timestamps se convierten a UTC
    - se elimina PII del vendedor (no relevante para análisis)
    """
    df_raw = spark.read.parquet(f"{bronze_path}/ventas_tiendas/fecha={RUN_DATE}/")

    log_quality(df_raw, "bronze_raw", "ventas_tiendas")

    df = (
        df_raw
        # Renombrar columnas del POS a nombres canónicos
        .withColumnRenamed("id_transaccion","venta_id")
        .withColumnRenamed("codigo_tienda","tienda_id")
        .withColumnRenamed("nombre_tienda","tienda_nombre")
        .withColumnRenamed("codigo_producto","sku")
        .withColumnRenamed("qty","cantidad")
        .withColumnRenamed("precio","precio_unitario")
        .withColumnRenamed("total","monto_total")
        .withColumnRenamed("fecha_hora_venta","vendido_en")
        .withColumnRenamed("id_cliente","cliente_id")

        # Estandarización
        .withColumn("sku",    F.upper(F.trim(F.col("sku"))))
        .withColumn("canal",  F.lit("tienda_fisica"))
        .withColumn("moneda", F.lit("MXN"))

        # Convertir timestamp a UTC (los POS envían en hora local México)
        .withColumn(
            "vendido_en",
            F.to_utc_timestamp(F.col("vendido_en"), "America/Mexico_City")
        )

        # Tipos correctos
        .withColumn("cantidad",F.col("cantidad").cast(IntegerType()))
        .withColumn("precio_unitario", F.col("precio_unitario").cast(DecimalType(10, 2)))
        .withColumn("monto_total",F.col("monto_total").cast(DecimalType(12, 2)))

        # Metadata de auditoría
        .withColumn("_fuente",       F.lit("bronze/ventas_tiendas"))
        .withColumn("_procesado_en", F.lit(RUN_TS).cast(TimestampType()))
    )

    # Validaciones de calidad
    df = validar_no_vacios(df, ["venta_id", "sku", "tienda_id"])
    df = df.filter(F.col("monto_total") > 0)
    df = df.filter(F.col("cantidad") > 0)


    df = mask_pii(df, "cliente_id")

    df = deduplicar(df, llave="venta_id", orden="_procesado_en")

    log_quality(df, "silver_clean", "ventas_tiendas")

    return df.select([f.name for f in SCHEMA_VENTAS_SILVER])


# ---------------------------------------------------------------------------
# Transformación: Ventas e-commerce (Bronze → Silver)
# ---------------------------------------------------------------------------

def transformar_ventas_ecommerce(bronze_path: str) -> DataFrame:
    """
    Lee ventas del canal digital desde Bronze y las normaliza
    al mismo esquema canónico que tiendas físicas.

    Diferencias clave respecto al POS:
    - No tiene tienda_id (venta online no tiene ubicación física)
    - Los timestamps ya vienen en UTC desde el webhook
    """
    df_raw = spark.read.parquet(f"{bronze_path}/ventas_ecommerce/fecha={RUN_DATE}/")

    log_quality(df_raw, "bronze_raw", "ventas_ecommerce")

    df = (
        df_raw
        .withColumnRenamed("order_id","venta_id")
        .withColumnRenamed("product_sku","sku")
        .withColumnRenamed("quantity","cantidad")
        .withColumnRenamed("unit_price","precio_unitario")
        .withColumnRenamed("total_amount","monto_total")
        .withColumnRenamed("created_at","vendido_en")
        .withColumnRenamed("customer_email","cliente_id")

        # E-commerce no tiene tienda física
        .withColumn("tienda_id",F.lit(None).cast(StringType()))
        .withColumn("tienda_nombre", F.lit(None).cast(StringType()))
        .withColumn("canal", F.lit("ecommerce"))
        .withColumn("moneda", F.lit("MXN"))

        .withColumn(
            "precio_unitario",
            (F.col("precio_unitario") / 100).cast(DecimalType(10, 2))
        )
        .withColumn(
            "monto_total",
            (F.col("monto_total") / 100).cast(DecimalType(12, 2))
        )

        .withColumn("sku",      F.upper(F.trim(F.col("sku"))))
        .withColumn("cantidad", F.col("cantidad").cast(IntegerType()))
        .withColumn("vendido_en", F.col("vendido_en").cast(TimestampType()))

        .withColumn("_fuente",       F.lit("bronze/ventas_ecommerce"))
        .withColumn("_procesado_en", F.lit(RUN_TS).cast(TimestampType()))
    )

    # Validaciones
    df = validar_no_vacios(df, ["venta_id", "sku"])
    df = df.filter(F.col("monto_total") > 0)
    df = df.filter(F.col("cantidad") > 0)

    # Enmascarar email del cliente (PII)
    df = mask_pii(df, "cliente_id")

    df = deduplicar(df, llave="venta_id", orden="_procesado_en")

    log_quality(df, "silver_clean", "ventas_ecommerce")

    return df.select([f.name for f in SCHEMA_VENTAS_SILVER])


# ---------------------------------------------------------------------------
# Transformación: Inventario ERP (Bronze → Silver)
# ---------------------------------------------------------------------------

def transformar_inventario(bronze_path: str) -> DataFrame:
    """
    Lee el snapshot de inventario del ERP desde Bronze.

    Reglas de negocio:
    - stock_disponible no puede ser negativo (datos del ERP a veces lo reportan así
      por timing de sincronización — se clampea a 0)
    - Se calcula stock_disponible real = total - reservado
    - sku se estandariza igual que ventas para permitir joins en Gold
    """
    df_raw = spark.read.parquet(f"{bronze_path}/inventario/fecha={RUN_DATE}/")

    log_quality(df_raw, "bronze_raw", "inventario")

    df = (
        df_raw
        .withColumnRenamed("inv_id","inventario_id")
        .withColumnRenamed("store_code","tienda_id")
        .withColumnRenamed("store_name","tienda_nombre")
        .withColumnRenamed("product_code","sku")
        .withColumnRenamed("qty_on_hand","stock_total")
        .withColumnRenamed("qty_reserved","stock_reservado")
        .withColumnRenamed("last_updated", "ultima_actualizacion")

        .withColumn("sku", F.upper(F.trim(F.col("sku"))))

        # Stock disponible = total - reservado, mínimo 0
        # El ERP puede reportar negativos por lag de sincronización interna
        .withColumn(
            "stock_disponible",
            F.greatest(
                F.col("stock_total") - F.col("stock_reservado"),
                F.lit(0)
            ).cast(IntegerType())
        )
        .withColumn("stock_reservado", F.col("stock_reservado").cast(IntegerType()))
        .withColumn(
            "ultima_actualizacion",
            F.to_utc_timestamp(F.col("ultima_actualizacion"), "America/Mexico_City")
        )

        .withColumn("_fuente",       F.lit("bronze/inventario"))
        .withColumn("_procesado_en", F.lit(RUN_TS).cast(TimestampType()))
    )

    df = validar_no_vacios(df, ["inventario_id", "tienda_id", "sku"])

    df = deduplicar(df, llave="sku", orden="ultima_actualizacion")

    log_quality(df, "silver_clean", "inventario")

    return df.select([f.name for f in SCHEMA_INVENTARIO_SILVER])


# ---------------------------------------------------------------------------
# Escritura a Silver (particionada por fecha)
# ---------------------------------------------------------------------------

def escribir_silver(df: DataFrame, tabla: str, silver_path: str) -> None:
    """
    Escribe el DataFrame a S3 en formato Parquet particionado por fecha.

    Particionamiento: year/month/day permite a Athena hacer partition pruning
    y reduce el costo de consultas históricas significativamente.

    Modo 'overwrite' por partición:esto es por idempotencia
    """
    (
        df
        .withColumn("year",  F.year(F.lit(RUN_DATE).cast("date")))
        .withColumn("month", F.month(F.lit(RUN_DATE).cast("date")))
        .withColumn("day",   F.dayofmonth(F.lit(RUN_DATE).cast("date")))
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(f"{silver_path}/{tabla}/")
    )
    print(f"Escrito: {silver_path}/{tabla}/year=*/month=*/day=*/")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print(f"[START] Silver job | run_date={RUN_DATE} | ts={RUN_TS.isoformat()}")

    # 1. Transformar ventas de tiendas físicas
    df_ventas_tiendas = transformar_ventas_tiendas(BRONZE)

    # 2. Transformar ventas de e-commerce
    df_ventas_ecommerce = transformar_ventas_ecommerce(BRONZE)

    # 3. Unificar ambas fuentes de ventas en una sola tabla Silver
    #    Este UNION es el corazón de la "vista unificada" que pide el negocio
    df_ventas_unificadas = df_ventas_tiendas.unionByName(df_ventas_ecommerce)
    print(f"Ventas unificadas: {df_ventas_unificadas.count()} registros")

    # 4. Transformar inventario
    df_inventario = transformar_inventario(BRONZE)

    # 5. Escribir a Silver
    escribir_silver(df_ventas_unificadas,"ventas",     SILVER)
    escribir_silver(df_inventario,"inventario", SILVER)

    print(f"Silver job completado exitosamente | ts={datetime.now(timezone.utc).isoformat()}")
    job.commit()


if __name__ == "__main__":
    main()
