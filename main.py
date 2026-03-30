import os
import logging
import requests
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
        logging.StreamHandler()  # Para ver también en consola
    ]
)

# ================================
# Modelos de datos para la API
# ================================
class AskRequest(BaseModel):
    email: EmailStr  # valida formato de email
    question: str = Field(..., min_length=1, max_length=2000)

class AskResponse(BaseModel):
    answer: str
    conversation_id: str = None

# ================================
# Inicialización de FastAPI y CORS
# ================================
app = FastAPI(title="Genie Proxy", version="1.0.0")

# Configura CORS para desarrollo (ajusta origins según el frontend que uses)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, restringir a tu dominio de GitHub Pages o dominio corporativo
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Después de definir AUTH_DOMAIN
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
    #    Documentación: https://docs.databricks.com/api/workspace/genie/startconversation
    url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_PAT}",
        "Content-Type": "application/json"
    }
    payload = {
        "content": request.question,
        "email": request.email   # Opcional, lo enviamos para que Genie lo registre
    }

    try:
        # Iniciar conversación (siempre crea una nueva; para mantener historial habría que manejar conversation_id)
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()  # Lanza excepción si status >= 400
        data = response.json()

        # La respuesta puede tener distintos formatos según el estado de Genie.
        # Según la API, devuelve un objeto con "conversation_id" y "answer" u otros campos.
        # Ajusta según la respuesta real que obtengas.
        answer = data.get("answer", "No se pudo obtener respuesta.")
        conversation_id = data.get("conversation_id")

        # Registro de éxito
        logging.info(f"Respuesta obtenida para {request.email}: {answer[:100]}...")

        return AskResponse(answer=answer, conversation_id=conversation_id)

    except requests.exceptions.Timeout:
        logging.error(f"Timeout al llamar a Genie para {request.email}")
        raise HTTPException(status_code=504, detail="Tiempo de espera agotado con Databricks")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al llamar a Genie: {e}")
        # Si se puede extraer el detalle de la respuesta, mejor
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
# Endpoint de salud (opcional)
# ================================
@app.get("/health")
async def health():
    return {"status": "ok"}

# ================================
# Si se ejecuta directamente (no recomendado, usar uvicorn)
# ================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
import json
logging.info(f"Respuesta completa de Databricks: {json.dumps(data, indent=2)}")
