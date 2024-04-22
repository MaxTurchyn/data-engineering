import os
import src.config
from src.export_raw import export_raw
from src.save_avro import save_avro
from fastapi import FastAPI, HTTPException

BASE_DIR = os.environ["BASE_DIR"]
RAW_DIR = os.path.join(BASE_DIR, "raw", "sales", "2022-08-09")
STG_DIR = os.path.join(BASE_DIR, "stg", "sales", "2022-08-09")

app_export_data = FastAPI()
app_save_avro = FastAPI()


@app_export_data.post("/export_raw/")
async def export_data():
    try:
        export_raw(RAW_DIR)
        return {"message": "Data exported successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app_save_avro.post("/save_avro/")
async def export_data():
    try:
        save_avro(RAW_DIR, STG_DIR)
        return {"message": "Data written successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    # uvicorn.run(app_export_data, host="0.0.0.0", port=8081)
    uvicorn.run(app_save_avro, host="0.0.0.0", port=8082)

# To make it visible in pull request