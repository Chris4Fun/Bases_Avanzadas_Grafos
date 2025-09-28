import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create one driver (connection pool) for the entire app
    app.state.driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD),
        # Optional pool tuning
        max_connection_pool_size=50,
        connection_acquisition_timeout=120.0,
        max_connection_lifetime=3600.0,
        fetch_size=1000,
    )
    try:
        yield
    finally:
        app.state.driver.close()

app = FastAPI(title="FastAPI + Neo4j Starter", lifespan=lifespan)

# CORS (adjust origins for your environment)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/locations/{id}")
def get_location(id: str):
    with app.state.driver.session() as session:
        record = session.execute_read(
            lambda tx: tx.run(
                "MATCH (l:Location {id:$id}) RETURN l", id=id
            ).single()
        )
        if not record:
            raise HTTPException(status_code=404, detail="Not found")
        return record["l"]

@app.post("/roads")
def upsert_road(payload: dict):
    # payload requires: {"from":"A","to":"B","km":10,"min":12}
    required = {"from", "to", "km", "min"}
    missing = required - set(payload.keys())
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing fields: {sorted(missing)}")
    with app.state.driver.session() as session:
        session.execute_write(
            lambda tx: tx.run(
                '''
                MATCH (a:Location {id:$from}), (b:Location {id:$to})
                MERGE (a)-[r:ROAD_TO]->(b)
                SET r.distance_km = $km, r.travel_min = $min
                ''',
                **payload
            )
        )
    return {"ok": True}
