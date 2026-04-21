import pandas as pd
from neo4j import GraphDatabase

NEO4J_URI = "bolt://34.56.251.47:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "passwordpassword"

BATCH_SIZE = 1000


def load(csv_path: str = "taxi_trips_clean.csv") -> None:
    df = pd.read_csv(csv_path)
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.run(
            "CREATE CONSTRAINT driver_id IF NOT EXISTS FOR (d:Driver) REQUIRE d.driver_id IS UNIQUE"
        )
        session.run(
            "CREATE CONSTRAINT company_name IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS UNIQUE"
        )
        session.run(
            "CREATE CONSTRAINT area_id IF NOT EXISTS FOR (a:Area) REQUIRE a.area_id IS UNIQUE"
        )

        rows = df.to_dict(orient="records")
        total = len(rows)
        for start in range(0, total, BATCH_SIZE):
            batch = rows[start : start + BATCH_SIZE]
            session.run(
                """
                UNWIND $rows AS row
                MERGE (d:Driver {driver_id: row.driver_id})
                MERGE (c:Company {name: row.company})
                MERGE (a:Area {area_id: toInteger(row.dropoff_area)})
                MERGE (d)-[:WORKS_FOR]->(c)
                CREATE (d)-[:TRIP {
                    trip_id: row.trip_id,
                    fare: toFloat(row.fare),
                    trip_seconds: toInteger(row.trip_seconds)
                }]->(a)
                """,
                rows=batch,
            )
            print(f"Loaded {min(start + BATCH_SIZE, total)}/{total}")

    driver.close()
    print("Done.")


if __name__ == "__main__":
    load()
