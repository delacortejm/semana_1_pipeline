import boto3
import pandas as pd
from io import StringIO
from pipeline.transform import clean_column_names, drop_nulls, filter_positive_values
from utils.logger import get_logger

logger = get_logger()
s3 = boto3.client('s3')

def lambda_handler(event, context):
    logger.info("Inicio del pipeline en Lambda")

    # 1. Obtener información del archivo subido
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    logger.info(f"Archivo recibido: s3://{bucket}/{key}")

    # 2. Descargar archivo desde S3
    response = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(response['Body'])

    # 3. Procesar datos
    df = clean_column_names(df)
    df = drop_nulls(df)
    df = filter_positive_values(df, 'precio_unitario')

    # 4. Guardar resultado como CSV en memoria
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # 5. Subir archivo procesado al bucket de salida
    output_key = key.replace("raw/", "processed/")
    s3.put_object(
        Bucket='ventas-pipeline-output',
        Key=output_key,
        Body=csv_buffer.getvalue()
    )

    logger.info(f"Archivo procesado guardado en: s3://ventas-pipeline-output/{output_key}")
    return {"statusCode": 200, "body": "Pipeline ejecutado correctamente"}

