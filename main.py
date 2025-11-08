from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import boto3
import json
import uuid
from botocore.exceptions import NoCredentialsError, ClientError
 
app = FastAPI()
 
# Cliente S3
s3 = boto3.client('s3')
 
# Nombre del bucket
BUCKET_NAME = "fastapi-ec2-jpcb"
 
# Modelo de datos
class Persona(BaseModel):
    nombre: str = Field(..., example="Juan")
    edad: int = Field(..., ge=0, example=25)
    correo: str = Field(..., example="juan@example.com")
 
@app.post("/insert")
def insert_persona(persona: Persona):
    """
    Recibe los datos de una persona, los almacena como JSON en S3
    y retorna la cantidad de archivos existentes en el bucket.
    """
    try:
        # Convertir persona a JSON
        data_json = json.dumps(persona.dict())
        file_name = f"{uuid.uuid4()}.json"
 
        # Subir archivo al bucket
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=data_json,
            ContentType="application/json"
        )
 
        # Listar archivos en el bucket
        response = s3.list_objects_v2(Bucket=BUCKET_NAME)
        total_files = response.get('KeyCount', 0)
 
        return {
            "message": f"Archivo '{file_name}' subido correctamente a {BUCKET_NAME}",
            "total_archivos": total_files
        }
 
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="No se encontraron credenciales de AWS configuradas.")
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error al interactuar con S3: {str(e)}")