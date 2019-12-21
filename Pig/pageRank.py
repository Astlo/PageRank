#!/usr/bin/python
# coding: utf-8

from org.apache.pig.scripting import *

P = Pig.compile("""
A = LOAD '$link_in' using PigStorage(' ') as (url:chararray, link:chararray);
ranks = LOAD '$rank_in' using PigStorage(' ') as (url:chararray, rank:float);

B = GROUP A by url;
-- après avoir classe le fichier par url, on regroupe les deux fichiers dans une variable previous_pagerank pour avoir accès à toutes les infos
previous_pagerank = JOIN ranks by url, B BY group;
previous_pagerank = FOREACH previous_pagerank GENERATE $0 as url, $1 as pagerank, $3.link as links;                                                                                               
-- pour chaque ligne du fichier précédent on génère la ligne (valeur de la contribution, site bénéficiaire) :
outbound_pagerank =
    FOREACH previous_pagerank
    GENERATE
        pagerank / COUNT ( links ) AS pagerank,
        FLATTEN ( links ) AS to_url;
-- on génère ensuite le nouveau rang de chaque site en faisant la jointure entre previous_pagerank et outbound_pagerank,
-- et on génère pour chaque site son nouveau rang sous le format (site,rang)
new_pagerank =
    FOREACH
        ( COGROUP outbound_pagerank BY to_url, previous_pagerank BY url INNER )
    GENERATE
        group AS url,
        0.15 + 0.85 * SUM ( outbound_pagerank.pagerank ) AS pagerank;
        
STORE new_pagerank
    INTO '$docs_out'
    USING PigStorage(' ');
""")

params = { 'link_in': '../4000/urls.txt', 'rank_in': '../4000/ranks.txt' }

for i in range(10):
    out = "4000/pagerank_data_" + str(i + 1) 
    params["docs_out"] = out
    Pig.fs("rmr " + out)
    stats = P.bind(params).runSingle()
    params["rank_in"] = out
    if not stats.isSuccessful():
        raise 'failed'

