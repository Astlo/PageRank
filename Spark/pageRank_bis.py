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
import os
from operator import add

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


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pageRank.py <file_Url_Urls>.txt <file_Url_Rang>.txt <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialisation du contexte de Spark.
    sc = SparkContext()

    os.makedirs('resultats', exist_ok=True)

    # Le fichier d'entrée pour les liens entre les sites doit être sous le format:
    # [URL] [URL référencée]
    # [URL] [URL référencée]
    # [URL] [URL référencée]
    # ...	

    lignes_url_urls = sc.textFile(sys.argv[1])	
    lignes_url_rang = sc.textFile(sys.argv[2])

    # On charges les urls du fichier des liens en entrée et on initialise leurs voisins.
    #liens = lignes_url_urls.map(lambda urls: splitUrlsref(urls)).distinct().groupByKey()
    liens = lignes_url_urls.map(lambda ligne: ligne.split(r'\s+')).map(lambda url1_url2: (url1_url2[0], url1_url2[1])).groupByKey()

    # On charge le rang pour chaque url du fichier des rangs en entrée.
    rangs = lignes_url_rang.map(lambda url_rang: splitRangs(url_rang))

    # On utilise le pageRank le nombre d'itérations indiquées.
    for i in range(int(sys.argv[3])):

        # MAP : On map la contribution d'une url avec les urls qu'elle référence.
        # liens.join(rangs) donne un couple (url,(urlsRéférencées, rang))
        contribs = liens.join(rangs).flatMap(lambda url_urls_rang: computeContribs(url_urls_rang[1][0], url_urls_rang[1][1]))

        # REDUCE : On additionne toutes les contributions qu'une url à reçue, puis on recalcul le rang pour cette même url à l'aide du damping factor (ici égal à 0.85)
        rangs = contribs.reduceByKey(add).mapValues(lambda rang: rang * 0.85 + 0.15)

    rangs = rangs.sortByKey()
    file_name = "resultats/resultat_iteration_{}.txt".format(sys.argv[3])
    fileRes = open(file_name, 'w')

    # On écrit dans un fichier le nouveau rang pour chaque url.
    for (lien, rang) in rangs.collect():
        fileRes.write(str(lien) + ' ' + str(rang) + '\n')
