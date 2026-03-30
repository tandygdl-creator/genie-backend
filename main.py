from fastapi import FastAPI
from pydantic import BaseModel
import requests
import re

app = FastAPI()

# ⚠️ CONFIGURACIÓN
CORPORATE_DOMAIN = "@empresa.com"
DATABRICKS_HOST = "https://adb-XXXXXXXX.azuredatabricks.net"
GENIE_SPACE_ID = "TU_SPACE_ID"
DATABRICKS_TOKEN = "TOKEN_DEL_SERVICE_ACCOUNT"

class Question(BaseModel):
    email: str
    question: str

def email_allowed(email: str) -> bool:
    return email.endswith(CORPORATE_DOMAIN)

@app.post("/ask")
def ask_genie(data: Question):
    if not email_allowed(data.email):
        return {"error": "Correo no autorizado"}

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "space_id": GENIE_SPACE_ID,
        "messages": [
            {"role": "user", "content": data.question}
        ]
    }

    r = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/genie/conversations",
        headers=headers,
        json=payload
    )

    if r.status_code != 200:
        return {"error": "Error llamando a Genie"}

    answer = r.json()["messages"][-1]["content"]
    return {"answer": answer}
