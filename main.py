import os
import logging
import requests
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr, Field
from fastapi.middleware.cors import CORSMiddleware

# ================================
# Configuración desde variables de entorno
# ================================
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://adb-8324102406929086.6.azuredatabricks.net")
DATABRICKS_PAT = os.getenv("DATABRICKS_PAT")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f082c07f49138b93fe35962b81617e")
AUTH_DOMAIN = os.getenv("AUTH_DOMAIN", "ab-inbev.com")
LOG_FILE = os.getenv("LOG_FILE", "usage.log")

# Verificar que el PAT esté presente
if not DATABRICKS_PAT:
    raise RuntimeError("La variable de entorno DATABRICKS_PAT no está definida")

# ================================
# Configuración de logging
# ================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ================================
# Modelos de datos para la API
# ================================
class AskRequest(BaseModel):
    email: EmailStr
    question: str = Field(..., min_length=1, max_length=2000)

class AskResponse(BaseModel):
    answer: str
    conversation_id: str = None

# ================================
# Inicialización de FastAPI y CORS
# ================================
app = FastAPI(title="Genie Proxy", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================================
# Logs de depuración
# ================================
print(f"* AUTH_DOMAIN inicializado: '{AUTH_DOMAIN}'")

def is_authorized_email(email: str) -> bool:
    result = email.lower().endswith(f"@{AUTH_DOMAIN.lower()}")
    print(f"Validando: email='{email}', dominio='{AUTH_DOMAIN}', resultado={result}")
    return result

# ================================
# Endpoint principal
# ================================
@app.post("/ask", response_model=AskResponse)
async def ask_genie(request: AskRequest):
    # 1. Validar dominio
    if not is_authorized_email(request.email):
        logging.warning(f"Intento no autorizado con email: {request.email}")
        raise HTTPException(status_code=403, detail="Correo no autorizado")

    # 2. Registrar la consulta
    logging.info(f"Usuario {request.email} pregunta: {request.question}")

    # 3. Construir la llamada a Databricks Genie
    url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_PAT}",
        "Content-Type": "application/json"
    }
    payload = {
        "content": request.question,
        "email": request.email
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Log de la respuesta completa (para depuración)
        logging.info(f"Respuesta completa de Databricks: {json.dumps(data, indent=2)}")

        # Por ahora devolvemos el JSON completo para ver la estructura
        # Luego ajustaremos para extraer el campo correcto
        return AskResponse(
            answer=json.dumps(data, indent=2),
            conversation_id=data.get("conversation_id")
        )

    except requests.exceptions.Timeout:
        logging.error(f"Timeout al llamar a Genie para {request.email}")
        raise HTTPException(status_code=504, detail="Tiempo de espera agotado con Databricks")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al llamar a Genie: {e}")
        detail = "Error al comunicarse con Databricks"
        if e.response is not None:
            try:
                detail = e.response.json().get("message", detail)
            except:
                detail = e.response.text
        raise HTTPException(status_code=502, detail=f"Error en Databricks: {detail}")
    except Exception as e:
        logging.exception(f"Error inesperado: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

# ================================
# Endpoint de salud
# ================================
@app.get("/health")
async def health():
    return {"status": "ok"}

# ================================
# Ejecución directa (para pruebas locales)
# ================================
if _name_ == "_main_":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
