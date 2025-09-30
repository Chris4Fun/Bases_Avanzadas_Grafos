from fastapi import APIRouter, HTTPException, Depends, Query
from neo4j import Session
from app.db import get_session

from pydantic import BaseModel, Field
from typing import Optional

class RoadUpsert(BaseModel):
    from_id: int = Field(alias="from")
    to_id: int = Field(alias="to")
    m: float = Field(description="Distance in meters")
    name: Optional[str] = None
    type: Optional[str] = None
    oneway: Optional[bool] = False

router = APIRouter(prefix="/roads", tags=["roads"])

# Create or update a road
@router.post("")
def upsert_road(payload: RoadUpsert, session: Session = Depends(get_session)):
    cypher = """
    MATCH (a:OSM_NODE {OSMID:$from}), (b:OSM_NODE {OSMID:$to})
    MERGE (a)-[r:OSM_ROAD]->(b)
    SET r.DISTANCE_METERS = $distance,
        r.ROAD_NAME       = coalesce($road_name, r.ROAD_NAME),
        r.ROAD_TYPE       = coalesce($road_type, r.ROAD_TYPE),
        r.ONEWAY          = coalesce($oneway, r.ONEWAY),
        r.FROM            = $from,
        r.TO              = $to
    RETURN r
    """
    params = {
        "from": payload.from_id,
        "to": payload.to_id,
        "distance": payload.m,
        "road_name": payload.name,
        "road_type": payload.type,
        "oneway": payload.oneway,
    }

    summary = session.execute_write(lambda tx: tx.run(cypher, params).consume())
    c = summary.counters

    if not c.contains_updates:
        raise HTTPException(status_code=404, detail="Road relation was not created")

    return {"ok": True,
            "created": c.relationships_created,
            "properties_set": c.properties_set}

# Read a road
@router.get("")
def read_road(
    frm: int = Query(alias="from"),
    to: int = Query(...),
    session: Session = Depends(get_session),
):
    rec = session.run(
        """
        MATCH (a:OSM_NODE {OSMID:$from})-[r:OSM_ROAD]->(b:OSM_NODE {OSMID:$to})
        RETURN a, b, r
        """,
        {"from": frm, "to": to},
    ).single()

    if not rec:
        raise HTTPException(status_code=404, detail="Not found")

    return {
        "FROM": rec["r"].get("FROM"),
        "TO": rec["r"].get("TO"),
        "DISTANCE_METERS": rec["r"].get("DISTANCE_METERS"),
        "ROAD_NAME": rec["r"].get("ROAD_NAME"),
        "ROAD_TYPE": rec["r"].get("ROAD_TYPE"),
        "ONEWAY": rec["r"].get("ONEWAY"),
    }


# Update road properties
@router.patch("")
def update_road(payload: dict, session: Session = Depends(get_session)):
    required = {"from", "to"}
    if not required.issubset(payload):
        raise HTTPException(
            status_code=400,
            detail=f"Missing fields: {sorted(required - set(payload))}",
        )

    session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (a:OSM_NODE {OSMID:$from})-[r:OSM_ROAD]->(b:OSM_NODE {OSMID:$to})
            SET r.DISTANCE_METERS = coalesce($distance, r.DISTANCE_METERS),
                r.ROAD_NAME       = coalesce($road_name, r.ROAD_NAME),
                r.ROAD_TYPE       = coalesce($road_type, r.ROAD_TYPE),
                r.ONEWAY          = coalesce($oneway, r.ONEWAY),
                r.FROM            = $from,
                r.TO              = $to
            """,
            payload,
        )
    )
    return {"ok": True}


# Delete a road
@router.delete("")
def delete_road(
    frm: int = Query(alias="from"),
    to: int = Query(...),
    session: Session = Depends(get_session),
):
    session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (a:OSM_NODE {OSMID:$from})-[r:OSM_ROAD]->(b:OSM_NODE {OSMID:$to})
            DELETE r
            """,
            {"from": frm, "to": to},
        )
    )
    return {"ok": True}