# Fortin Guillaume
# Brohan Romain
# UFR Sciences et Techniques de Nantes
# 2018 - 2019
# Large Scale Data Management
# PageRank
"""
Ceci est le pagerank que nous devions implémenter en spark dans le cadre de notre projet en Large Scale Data Management.

Utilisation:
spark-submit pageRank.py [urls].txt [ranks].txt [nbIteration]
"""
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pageRank.py <file_Url_Urls>.txt <file_Url_Rang>.txt <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialisation du contexte de Spark.
    #conf = SparkConf().setMaster("local[2]").setAppName("pageRank")
    spark = SparkSession\
        .builder\
        .appName("pageRank")\
        .getOrCreate()

    # Le fichier d'entrée pour les liens entre les sites doit être sous le format:
    # [URL] [URL référencée]
    # [URL] [URL référencée]
    # [URL] [URL référencée]
    # ...	

    #lignes_url_urls = sc.textFile(sys.argv[1])	
    lignes_url_urls = spark.read.option("delimiter"," ").csv(sys.argv[1]).cache()
    lignes_url_rang = spark.read.option("delimiter"," ").csv(sys.argv[2])
	
    Tcount = lignes_url_urls.groupBy("_c0").count()
    df = Tcount.join(lignes_url_urls, "_c0").join(lignes_url_rang, "_c0").cache()

    names = ["url","nbUrl","urls","rang"]
    df = df.toDF(*names)
    newdf = df[df.columns[1:4]].cache()
    newdf = newdf.withColumn('contrib', newdf["rang"]/newdf["nbUrl"])
    newdf = newdf.groupBy("urls").sum("contrib")
    newdf = newdf.withColumn('rang', newdf["sum(contrib)"]*0.85+0.15)
    newdf.select([c for c in newdf.columns if c in ['urls', 'rang']]).sort(desc("rang")).write.csv("resultat_bis")

