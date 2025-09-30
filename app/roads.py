from fastapi import APIRouter, HTTPException, Depends, Query, Request
from pydantic import BaseModel, Field
from neo4j import Session

router = APIRouter(prefix="/roads", tags=["roads"])

def get_session(request: Request):
    driver = request.app.state.driver
    with driver.session() as session:
        yield session

class RoadUpsert(BaseModel):
    from_: str = Field(alias="from")
    to: str
    km: float | None = None
    min: float | None = None

    model_config = {"populate_by_name": True}

@router.post("")
def upsert_road(payload: RoadUpsert, session: Session = Depends(get_session)):
    session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (a:Location {id:$from}), (b:Location {id:$to})
            MERGE (a)-[r:ROAD_TO]->(b)
            SET r.distance_km = $km, r.travel_min = $min
            """,
            **{"from": payload.from_, "to": payload.to, "km": payload.km, "min": payload.min}
        )
    )
    return {"ok": True}

@router.get("")
def read_road(
    frm: str = Query(alias="from"),
    to: str = Query(...),
    session: Session = Depends(get_session),
):
    rec = session.run(
        """
        MATCH (a:Location {id:$from})-[r:ROAD_TO]->(b:Location {id:$to})
        RETURN a,b,r
        """,
        **{"from": frm, "to": to}
    ).single()
    if not rec:
        raise HTTPException(status_code=404, detail="Not found")
    return {"from": rec["a"], "to": rec["b"], "road": rec["r"]}

@router.patch("")
def update_road(
    payload: dict,
    session: Session = Depends(get_session),
):
    required = {"from", "to"}
    if not required.issubset(payload):
        raise HTTPException(status_code=400, detail=f"Missing fields: {sorted(required - set(payload))}")

    session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (a:Location {id:$from})-[r:ROAD_TO]->(b:Location {id:$to})
            SET r.distance_km = coalesce($km, r.distance_km),
                r.travel_min = coalesce($min, r.travel_min)
            """,
            **payload
        )
    )
    return {"ok": True}

@router.delete("")
def delete_road(
    frm: str = Query(alias="from"),
    to: str = Query(...),
    session: Session = Depends(get_session),
):
    session.execute_write(
        lambda tx: tx.run(
            """
            MATCH (a:Location {id:$from})-[r:ROAD_TO]->(b:Location {id:$to})
            DELETE r
            """,
            **{"from": frm, "to": to}
        )
    )
    return {"ok": True}