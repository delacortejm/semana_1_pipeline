import argparse
from pipeline.load import load_data
from pipeline.save import save_csv
from pipeline.transform import clean_column_names, drop_nulls, filter_positive_values
from utils.logger import get_logger

logger = get_logger()

def main(input_path, output_path):
    logger.info("Inicio del pipeline")

    logger.info("Cargando datos desde {input_path}")
    df = load_data(input_path)

    logger.info("Transformando datos")
    df = clean_column_names(df)
    df = drop_nulls(df)
    df = filter_positive_values(df, "ventas")  # ejemplo

    logger.info("Guardando datos transformados")
    save_csv(df, output_path)

    logger.info("Pipeline finalizado con éxito")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de transformación de datos")
    parser.add_argument("--input", required=True, help="Ruta al archivo de entrada (CSV)")
    parser.add_argument("--output", required=True, help="Ruta al archivo de salida (CSV)")

    args = parser.parse_args()

    main(args.input, args.output)