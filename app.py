from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, round as spark_round

NEO4J_URI = "bolt://34.56.251.47:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "passwordpassword"

CSV_PATH = "taxi_trips_clean.csv"


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("cs498-hw4").getOrCreate()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
    )
    app.state.spark = get_spark()
    yield
    app.state.driver.close()
    app.state.spark.stop()


app = FastAPI(lifespan=lifespan)


def run(query: str, **params):
    with app.state.driver.session() as session:
        return list(session.run(query, **params))


@app.get("/graph-summary")
def graph_summary():
    rec = run(
        """
        MATCH (d:Driver) WITH count(d) AS driver_count
        MATCH (c:Company) WITH driver_count, count(c) AS company_count
        MATCH (a:Area) WITH driver_count, company_count, count(a) AS area_count
        MATCH ()-[t:TRIP]->()
        RETURN driver_count, company_count, area_count, count(t) AS trip_count
        """
    )[0]
    return {
        "driver_count": rec["driver_count"],
        "company_count": rec["company_count"],
        "area_count": rec["area_count"],
        "trip_count": rec["trip_count"],
    }


@app.get("/top-companies")
def top_companies(n: int = Query(..., ge=1)):
    recs = run(
        """
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(:Area)
        RETURN c.name AS name, count(t) AS trip_count
        ORDER BY trip_count DESC
        LIMIT $n
        """,
        n=n,
    )
    return {
        "companies": [{"name": r["name"], "trip_count": r["trip_count"]} for r in recs]
    }


@app.get("/high-fare-trips")
def high_fare_trips(area_id: int, min_fare: float):
    recs = run(
        """
        MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
        WHERE t.fare > $min_fare
        RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
        ORDER BY fare DESC
        """,
        area_id=area_id,
        min_fare=min_fare,
    )
    return {
        "trips": [
            {"trip_id": r["trip_id"], "fare": r["fare"], "driver_id": r["driver_id"]}
            for r in recs
        ]
    }


@app.get("/co-area-drivers")
def co_area_drivers(driver_id: str):
    recs = run(
        """
        MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
        WHERE d1 <> d2
        WITH d2, count(DISTINCT a) AS shared_areas
        RETURN d2.driver_id AS driver_id, shared_areas
        ORDER BY shared_areas DESC
        """,
        driver_id=driver_id,
    )
    return {
        "co_area_drivers": [
            {"driver_id": r["driver_id"], "shared_areas": r["shared_areas"]}
            for r in recs
        ]
    }


@app.get("/avg-fare-by-company")
def avg_fare_by_company():
    recs = run(
        """
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(:Area)
        RETURN c.name AS name, round(avg(t.fare), 2) AS avg_fare
        ORDER BY avg_fare DESC
        """
    )
    return {"companies": [{"name": r["name"], "avg_fare": r["avg_fare"]} for r in recs]}


@app.get("/area-stats")
def area_stats(area_id: int):
    spark = get_spark()
    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
    agg = (
        df.filter(col("dropoff_area") == area_id)
        .groupBy()
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("fare"), 2).alias("avg_fare"),
            spark_round(avg("trip_seconds"), 0).alias("avg_trip_seconds"),
        )
        .collect()
    )
    row = agg[0]
    return {
        "area_id": area_id,
        "trip_count": int(row["trip_count"]),
        "avg_fare": float(row["avg_fare"]) if row["avg_fare"] is not None else 0.0,
        "avg_trip_seconds": int(row["avg_trip_seconds"])
        if row["avg_trip_seconds"] is not None
        else 0,
    }


@app.get("/top-pickup-areas")
def top_pickup_areas(n: int = Query(..., ge=1)):
    spark = get_spark()
    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
    rows = (
        df.groupBy("pickup_area")
        .agg(count("*").alias("trip_count"))
        .orderBy(col("trip_count").desc())
        .limit(n)
        .collect()
    )
    return {
        "areas": [
            {"pickup_area": int(r["pickup_area"]), "trip_count": int(r["trip_count"])}
            for r in rows
        ]
    }


@app.get("/company-compare")
def company_compare(company1: str, company2: str):
    spark = get_spark()
    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
    df = df.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / 60.0))
    df.createOrReplaceTempView("trips")

    def _esc(s: str) -> str:
        return s.replace("'", "''")

    query = f"""
        SELECT company,
               COUNT(*) AS trip_count,
               ROUND(AVG(fare), 2) AS avg_fare,
               ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
               ROUND(AVG(trip_seconds), 0) AS avg_trip_seconds
        FROM trips
        WHERE company IN ('{_esc(company1)}', '{_esc(company2)}')
        GROUP BY company
    """
    rows = spark.sql(query).collect()
    found = {r["company"] for r in rows}
    if company1 not in found or company2 not in found:
        return {"error": "one or more companies not found"}
    return {
        "comparison": [
            {
                "company": r["company"],
                "trip_count": int(r["trip_count"]),
                "avg_fare": float(r["avg_fare"]),
                "avg_fare_per_minute": float(r["avg_fare_per_minute"]),
                "avg_trip_seconds": int(r["avg_trip_seconds"]),
            }
            for r in rows
        ]
    }
