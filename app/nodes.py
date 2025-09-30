from fastapi import APIRouter, HTTPException, Depends, Query
from neo4j import Session
from app.db import get_session

from pydantic import BaseModel, Field
from typing import Optional

class NodeUpsert(BaseModel):
    osmid: int
    lat: float
    lon: float

router = APIRouter(prefix="/nodes", tags=["nodes"])

# Create or update a node
@router.post("")
def upsert_node(payload: NodeUpsert, session: Session = Depends(get_session)):
    cypher = """
    MERGE (n:OSM_NODE {OSMID:$osmid})
    SET n.LAT = $lat,
        n.LON = $lon
    RETURN n
    """
    params = {
        "osmid": payload.osmid,
        "lat": payload.lat,
        "lon": payload.lon,
    }

    summary = session.execute_write(lambda tx: tx.run(cypher, params).consume())
    c = summary.counters

    if not c.contains_updates:
        raise HTTPException(status_code=500, detail="Node was not created/updated")

    return {"ok": True,
            "created": c.nodes_created,
            "properties_set": c.properties_set}


# Read a node
@router.get("")
def read_node(
    osmid: int = Query(...),
    session: Session = Depends(get_session),
):
    rec = session.run(
        """
        MATCH (n:OSM_NODE {OSMID:$osmid})
        RETURN n
        """,
        {"osmid": osmid},
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Not found")

    node = rec["n"]
    return {
        "OSMID": node.get("OSMID"),
        "LAT": node.get("LAT"),
        "LON": node.get("LON"),
    }


# Update a node
@router.patch("")
def update_node(payload: dict, session: Session = Depends(get_session)):
    if "osmid" not in payload:
        raise HTTPException(status_code=400, detail="Missing field: osmid")

    params = {
        "osmid": payload["osmid"],
        "lat": payload.get("lat", None),
        "lon": payload.get("lon", None),
    }

    session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (n:OSM_NODE {OSMID:$osmid})
            SET n.LAT = coalesce($lat, n.LAT),
                n.LON = coalesce($lon, n.LON)
            """,
            params,
        )
    )
    return {"ok": True}


# Delete a node
@router.delete("")
def delete_node(
    osmid: int = Query(...),
    session: Session = Depends(get_session),
):
    session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (n:OSM_NODE {OSMID:$osmid})
            DETACH DELETE n
            """,
            {"osmid": osmid},
        )
    )
    return {"ok": True}
