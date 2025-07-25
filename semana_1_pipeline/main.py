import argparse
from pipeline.load import load_data
from pipeline.save import save_csv
from pipeline.transform import clean_column_names, drop_nulls, filter_positive_values, remove_empty_names, normalize_product_names
from utils.logger import get_logger

logger = get_logger()

def main(input_path, output_path):
    logger.info("Inicio del pipeline")

    try:
        logger.info("Cargando datos desde {input_path}")
        df = load_data(input_path)

        logger.info("Transformando datos")
        df = clean_column_names(df)
        df = drop_nulls(df)
        df = filter_positive_values(df)
        df = remove_empty_names(df)
        df = normalize_product_names(df)
        
        logger.info("Guardando datos transformados en {output_path}")
        save_csv(df, output_path)
        
        logger.info("Pipeline finalizado con éxito")
    
    except FileNotFoundError:
        logger.error(f"El archivo de entrada '{input_path}' no existe.")
        raise
    except ValueError as e:
        logger.error(f"Error de validación: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado en el pipeline: {str(e)}")
        raise