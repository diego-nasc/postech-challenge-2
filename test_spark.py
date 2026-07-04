import sys
import os

# ─── Aponta o Spark para o Python EXATO deste ambiente (conda postech2) ───
# Sem isto, os workers do Spark no Windows caem no alias da Microsoft Store
# e falham com "Python worker failed to connect back".
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Teste")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("Spark versão:", spark.version)
print("Python do Spark:", sys.executable)

df = spark.createDataFrame(
    [("MG", 1), ("SP", 1), ("MG", 0), ("BA", 1)],
    ["uf", "alfabetizado"],
)
print("Linhas:", df.count())
df.groupBy("uf").sum("alfabetizado").show()

spark.stop()
print("OK — Spark local funcionando.")