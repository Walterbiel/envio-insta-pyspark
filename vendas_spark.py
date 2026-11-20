import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)   # evita o erro "python3 not found" no Windows

spark = SparkSession.builder.appName("vendas_qg").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Dados de origem (em projeto real → spark.read.csv ou spark.read.parquet)
vendas = spark.createDataFrame(
    [
        (1, 1, "2026-01-05", "Notebook", 4500.0),
        (2, 1, "2026-01-05", "Mouse", 160.0),
        (3, 2, "2026-01-10", "Cadeira", 900.0),
        (3, 2, "2026-01-10", "Cadeira", 900.0),     # duplicada
        (4, 3, "2026-02-01", "Monitor", 2400.0),
        (5, 3, "2026-02-01", "Caneca", None),       # sem valor
        (6, 4, "2026-02-15", "Mesa", 650.0),
        (7, 2, "2026-03-02", "Mouse", 80.0),
    ],
    ["venda_id", "cliente_id", "data", "produto", "valor"],
)
clientes = spark.createDataFrame(
    [(1, " ana", "São Paulo"), (2, "bruno ", "Recife"), (3, "carla", "Salvador"), (4, "diego", "Curitiba")],
    ["cliente_id", "nome", "cidade"],
)

# 2. Limpeza
vendas_limpas = (
    vendas.dropDuplicates()
    .filter(F.col("valor").isNotNull())
    .withColumn("data", F.to_date("data"))
    .withColumn("mes", F.date_format("data", "yyyy-MM"))
)
clientes_limpos = clientes.withColumn("nome", F.initcap(F.trim("nome")))

# 3. Join
base = vendas_limpas.join(clientes_limpos, "cliente_id", "left")
base.show()

# 4. Agregação por cidade
por_cidade = base.groupBy("cidade").agg(
    F.sum("valor").alias("faturamento"),
    F.count("*").alias("vendas"),
)
por_cidade.orderBy(F.desc("faturamento")).show()

# 5. Window → maior venda de cada cliente
janela = Window.partitionBy("cliente_id").orderBy(F.desc("valor"))
maior_venda = base.withColumn("pos", F.row_number().over(janela)).filter("pos = 1").drop("pos")
maior_venda.select("nome", "produto", "valor").show()

# 6. Gravar particionado por mês → cada mês vira uma pasta
base.write.mode("overwrite").partitionBy("mes").parquet("saida/vendas")
print("Gravado em saida/vendas → uma pasta por mês")

spark.stop()
