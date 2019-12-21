oldRank = load 'data_prep.txt' as ( url: chararray, rank: float, links:{ link: ( url: chararray ) } );


sumRank = FOREACH oldRank GENERATE rank/COUNT(links) AS rank, FLATTEN(links) as to_url;
newRank = FOREACH( COGROUP sumRank BY to_url,oldRank BY url INNER) GENERATE group AS url, 0.15 + 0.85*SUM(sumRank.rank) as pagerank, FLATTEN(oldRank.links) AS links;

DUMP newRank;
