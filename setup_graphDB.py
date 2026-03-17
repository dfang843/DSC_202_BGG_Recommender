import pandas as pd
from neo4j import GraphDatabase

NEO4J_URI      = "neo4j://localhost:7687" # Update this as needed
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = ""                       # Update this password as needed

CSV_PATH = "path/to/bgg_data_filtered_with_complexity_binning.csv"  # Update this path as needed

def setup_graph(csv_path: str) -> None:
    """
    Builds the Neo4j graph from scratch:
      1. Wipes existing graph
      2. Creates indexes
      3. Loads CSV and creates nodes + relationships
      4. Sets IDF weights on MECHANIC nodes
      5. Prints verification summary
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    df = pd.read_csv(csv_path)

    with driver.session() as session:

        # STEP 0: Wipe existing graph
        print("Step 0: Wiping existing graph...")
        session.run("MATCH (n) DETACH DELETE n")
        print("  Done — all nodes and relationships deleted.\n")

        # STEP 1: Create indexes
        print("Step 1: Creating indexes...")
        session.run("CREATE INDEX game_name_idx IF NOT EXISTS FOR (g:GAME) ON (g.name)")
        session.run("CREATE INDEX mechanic_name_idx IF NOT EXISTS FOR (m:MECHANIC) ON (m.name)")
        session.run("CREATE INDEX domain_name_idx IF NOT EXISTS FOR (d:DOMAIN) ON (d.name)")
        print("  Indexes created for GAME, MECHANIC, DOMAIN.\n")

        # STEP 2: Load data and create nodes + relationships
        print("Step 2: Loading data and creating nodes + relationships...")
        print(f"  CSV rows to process: {len(df)}")
        # Filter out rows with missing required fields
        df_clean = df[
            df["name"].notna() &
            df["mechanic_name"].notna() &
            df["domain_name"].notna()
        ].copy()
        print(f"  Rows after filtering nulls: {len(df_clean)}")

        # Batch insert for performance
        batch_size = 1000
        total_rows = len(df_clean)
        records = df_clean.to_dict("records")

        for i in range(0, total_rows, batch_size):
            batch = records[i:i + batch_size]
            session.run("""
                UNWIND $rows AS row
                MERGE (g:GAME {name: row.name})
                ON CREATE SET
                    g.id                 = toInteger(row.id),
                    g.complexity_average = toFloat(row.complexity_average),
                    g.complexity_bin     = row.complexity_bin,
                    g.min_age            = toInteger(row.min_age),
                    g.min_players        = toInteger(row.min_players),
                    g.max_players        = toInteger(row.max_players),
                    g.rating_average     = toFloat(row.rating_average),
                    g.bgg_rank           = toInteger(row.bgg_rank),
                    g.owned_users        = toInteger(row.owned_users)
                MERGE (m:MECHANIC {name: row.mechanic_name})
                MERGE (d:DOMAIN   {name: row.domain_name})
                MERGE (g)-[:HAS_MECHANIC]->(m)
                MERGE (g)-[:SUBJECT]->(d)
            """, {"rows": batch})
            print(f"  Processed {min(i + batch_size, total_rows)}/{total_rows} rows...")

        print("  Done — nodes and relationships created.\n")

        # STEP 3: Set IDF weights on MECHANIC nodes
        print("Step 3: Computing and setting IDF weights on MECHANIC nodes...")
        result = session.run("""
            MATCH (g:GAME)
            WITH count(g) AS N
            MATCH (m:MECHANIC)<-[:HAS_MECHANIC]-(g:GAME)
            WITH m, count(DISTINCT g) AS df, N
            SET m.weight = log10(1.0 * (N + 1) / (df + 1))
            RETURN m.name AS mechanic, m.weight AS weight
            ORDER BY m.weight ASC
            LIMIT 5
        """)
        lowest = result.data()
        print("  IDF weights set. Lowest weighted mechanics (most common):")
        for row in lowest:
            print(f"    {row['mechanic']:<45} {row['weight']:.4f}")
        print()


        # STEP 4: Verification summary
        print("Step 4: Verification summary...")

        game_count = session.run(
            "MATCH (g:GAME) RETURN count(g) AS c"
        ).single()["c"]

        mechanic_count = session.run(
            "MATCH (m:MECHANIC) RETURN count(m) AS c"
        ).single()["c"]

        domain_count = session.run(
            "MATCH (d:DOMAIN) RETURN count(d) AS c"
        ).single()["c"]

        has_mechanic_count = session.run(
            "MATCH ()-[r:HAS_MECHANIC]->() RETURN count(r) AS c"
        ).single()["c"]

        subject_count = session.run(
            "MATCH ()-[r:SUBJECT]->() RETURN count(r) AS c"
        ).single()["c"]

        null_weights = session.run("""
            MATCH (m:MECHANIC)
            WHERE m.weight IS NULL
            RETURN count(m) AS c
        """).single()["c"]

        print(f"  GAME nodes:             {game_count}")
        print(f"  MECHANIC nodes:         {mechanic_count}")
        print(f"  DOMAIN nodes:           {domain_count}")
        print(f"  HAS_MECHANIC edges:     {has_mechanic_count}")
        print(f"  SUBJECT edges:          {subject_count}")
        print(f"  MECHANIC nodes missing weight: {null_weights}")

        # Sanity check: spot check a known game
        spot_check = session.run("""
            MATCH (g:GAME {name: "Catan"})-[:HAS_MECHANIC]->(m:MECHANIC)
            RETURN g.name AS game, collect(m.name) AS mechanics
        """).single()

        if spot_check:
            print(f"\n  Spot check — {spot_check['game']}:")
            print(f"    Mechanics: {sorted(spot_check['mechanics'])}")
        else:
            print("\n  Spot check — Gloomhaven not found, check data.")

    driver.close()
    print("\nGraph setup complete.")


if __name__ == "__main__":
    setup_graph(CSV_PATH)
