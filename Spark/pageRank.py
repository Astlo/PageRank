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
import re
import sys
from operator import add
import datetime

from pyspark import SparkContext

def computeContribs(urls, rank):
    """ """
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def splitUrlsref(urls):
    """ """
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def splitRangs(url_rang):
    """ """
    parts = re.split(r'\s+', url_rang)
    return parts[0], float(parts[1])

def g(x):
    print(x)

if __name__ == "__main__":
    # Initialisation du contexte de Spark.
    sc = SparkContext()

    # Le fichier d'entrée pour les liens entre les sites doit être sous le format:
    # [URL] [URL référencée]
    # [URL] [URL référencée]
    # [URL] [URL référencée]
    # ...	

    lignes_url_urls = sc.textFile('../10000/urls.txt')	
    lignes_url_rang = sc.textFile('../10000/ranks.txt')

    # On charges les urls du fichier des liens en entrée et on initialise leurs voisins.
    liens = lignes_url_urls.map(lambda urls: splitUrlsref(urls)).groupByKey()

    # On charge le rang pour chaque url du fichier des rangs en entrée.
    rangs = lignes_url_rang.map(lambda url_rang: splitRangs(url_rang))
    print(datetime.datetime.now())
    now = datetime.datetime.now()
    # On utilise le pageRank le nombre d'itérations indiquées.
    for i in range(10):

        # MAP : On map la contribution d'une url avec les urls qu'elle référence.
        # liens.join(rangs) donne un couple (url,(urlsRéférencées, rang))
        contribs = liens.join(rangs).flatMap(lambda url_urls_rang: computeContribs(url_urls_rang[1][0], url_urls_rang[1][1]))
                
        # REDUCE : On additionne toutes les contributions qu'une url à reçue, puis on recalcul le rang pour cette même url à l'aide du damping factor (ici égal à 0.85)
        rangs = contribs.reduceByKey(add).mapValues(lambda rang: rang * 0.85 + 0.15)

    
    #print(rangs.take(4))
    #print(rangs.count())
    rangs.saveAsTextFile("10000")
    print(datetime.datetime.now())
    print(datetime.datetime.now()-now)