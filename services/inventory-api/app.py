from fastapi import FastAPI, HTTPException, Query
import time

app = FastAPI()

INVENTORY = {
    "MANGO-12PK": 500,
    "ACAI-24PK": 150,
    "COCONUT-6PK": 0,
}


@app.get("/inventory/{sku}")
def get_inventory(
    sku: str,
    mode: str | None = Query(default=None),
):
    if mode == "500":
        raise HTTPException(
            status_code=500,
            detail="Internal inventory error",
        )

    if mode == "503":
        raise HTTPException(
            status_code=503,
            detail="Inventory service unavailable",
        )

    if mode == "429":
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
        )

    if mode == "slow":
        time.sleep(10)

    if sku not in INVENTORY:
        raise HTTPException(
            status_code=404,
            detail="SKU not found",
        )

    return {
        "sku": sku,
        "available_quantity": INVENTORY[sku],
    }