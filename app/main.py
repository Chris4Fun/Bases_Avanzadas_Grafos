import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
from dotenv import load_dotenv

from . import roads

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Function to create connection pool
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD),
        max_connection_pool_size=50,
        connection_acquisition_timeout=120.0,
        max_connection_lifetime=3600.0,
        fetch_size=1000,
    )
    try:
        if app.state.driver is None:
            raise Exception("Driver not created")
            
        print("Connecting to Neo4j...")
        yield
    finally:
        app.state.driver.close()

# API instance
app = FastAPI(title="FastAPI + Neo4j Starter", lifespan=lifespan)

@app.get("/health")
def health(request: Request):
    driver = request.app.state.driver
    with driver.session() as s:
        s.run("RETURN 1").consume()
    return {"db": "ok"}

#app.include_router(nodes.router)
app.include_router(roads.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)