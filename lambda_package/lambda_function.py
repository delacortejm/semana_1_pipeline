def lambda_handler(event, context):
    try:
        logger.info("Inicio del pipeline")
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        if not key.endswith(".csv"):
            logger.warning(f"Archivo ignorado: {key}")
            return {"statusCode": 200, "body": "Archivo no procesado (no CSV)"}

        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'])

        if df.empty:
            logger.warning("Archivo CSV está vacío")
            return {"statusCode": 200, "body": "Archivo vacío"}

        # Aplicar transformaciones
        df = clean_column_names(df)
        df = drop_nulls(df)
        df = filter_positive_values(df, 'precio_unitario')

        # Guardar en S3
        output_key = key.replace("raw/", "processed/")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket='ventas-pipeline-output', Key=output_key, Body=csv_buffer.getvalue())

        logger.info(f"Archivo procesado y guardado: {output_key}")
        return {"statusCode": 200, "body": "Proceso exitoso"}

    except pd.errors.EmptyDataError:
        logger.error("Error: archivo CSV vacío o ilegible")
        return {"statusCode": 400, "body": "Archivo CSV ilegible"}

    except KeyError as e:
        logger.error(f"Error: columna faltante {e}")
        return {"statusCode": 400, "body": f"Columna faltante: {str(e)}"}

    except Exception as e:
        logger.critical(f"Error inesperado: {e}")
        raise  