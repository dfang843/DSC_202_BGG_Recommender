import pandas as pd
from neo4j import GraphDatabase

NEO4J_URI      = "neo4j://hostname:7687"           # Update URI as needed (bolt:// or neo4j://)
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = ""                                # Update password as needed

GRAPH_QUERY = """
    MATCH (candidate:GAME)-[:HAS_MECHANIC]->(cm:MECHANIC)
    WHERE NOT candidate.name IN $likedGames
    WITH candidate, collect(DISTINCT cm) AS candMechanics,
         reduce(s = 0.0, m IN collect(DISTINCT cm) | s + m.weight) AS candWeight

    UNWIND $likedGames AS likedGame
    MATCH (seed:GAME {name: likedGame})-[:HAS_MECHANIC]->(sm:MECHANIC)
    WITH candidate, candMechanics, candWeight, collect(DISTINCT sm) AS seedMechanics

    WITH candidate, candWeight,
         reduce(i = 0.0, m IN [x IN candMechanics WHERE x IN seedMechanics] | i + m.weight) AS intersectionWeight,
         reduce(s = 0.0, m IN seedMechanics | s + m.weight) AS seedWeight

    WITH candidate, intersectionWeight, seedWeight, candWeight AS candidateWeight
    WITH candidate,
         CASE WHEN (seedWeight + candidateWeight - intersectionWeight) = 0.0 THEN 0.0
              ELSE intersectionWeight / (seedWeight + candidateWeight - intersectionWeight)
         END AS perSeedMechSim

    WITH candidate, collect(perSeedMechSim) AS mechSims
    UNWIND mechSims AS sim
    WITH candidate, avg(sim) AS mechSim

    MATCH (candidate)-[:SUBJECT]->(cd:DOMAIN)
    WITH candidate, mechSim, collect(DISTINCT cd.name) AS candDomains, $likedGames AS likedGames,
         {
           `Wargames`:          3252,
           `Strategy Games`:    2169,
           `Family Games`:      2062,
           `Thematic Games`:    1128,
           `Abstract Games`:     930,
           `Children's Games`:   728,
           `Party Games`:        549,
           `Customizable Games`: 251
         } AS domainFreqs

    MATCH (seed:GAME)-[:SUBJECT]->(sd:DOMAIN)
    WHERE seed.name IN likedGames
    WITH candidate, mechSim, candDomains, collect(DISTINCT sd.name) AS allSeedDomains, domainFreqs

    WITH candidate, mechSim,
         reduce(intersectWeight = 0.0, d IN [x IN candDomains WHERE x IN allSeedDomains] |
               intersectWeight + 1.0 / domainFreqs[d]) AS weightedIntersection,
         reduce(unionWeight = 0.0, d IN apoc.coll.union(candDomains, allSeedDomains) |
               unionWeight + 1.0 / domainFreqs[d]) AS weightedUnion

    WITH candidate, mechSim,
         CASE WHEN weightedUnion = 0.0 THEN 0.0
              ELSE weightedIntersection / weightedUnion
         END AS domainSim

    RETURN candidate.name           AS GameRecommendation,
           candidate.complexity_bin AS ComplexityBin,
           round(mechSim, 5)        AS MechanicsSimilarity,
           round(domainSim, 5)      AS DomainSimilarity,
           round(0.75 * mechSim + 0.25 * domainSim, 5) AS GraphSimilarity
    ORDER BY GraphSimilarity DESC
    LIMIT 100
"""

def get_graph_candidates(profile: dict, verbose: bool = False) -> pd.DataFrame:
    """
    Runs weighted graph similarity query against Neo4j using the user profile.
    Returns a DataFrame of top 100 candidate games with their similarity scores.
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        result = session.run(GRAPH_QUERY, {
            "likedGames":       profile["liked_games"],
        })
        records = result.data()

    driver.close()
    df = pd.DataFrame(records)

    # Print top 10 candidates
    if verbose:
        print("\n--- Top 10 Graph Candidates ---")
        print(df.head(10).to_string(index=False))

    return df