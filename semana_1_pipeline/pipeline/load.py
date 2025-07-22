import pandas as pd

def load_csv(path: str) -> pd.DataFrame:
    """
    Lee un archivo CSV y devuelve un DataFrame.

    Args:
        path (str): Ruta al archivo CSV.

    Returns:
        pd.DataFrame: Datos cargados.
    """
    return pd.read_csv(path)

def load_data(path: str) -> pd.DataFrame:
    if path.endswith(".csv"):
        return pd.read_csv(path)
    elif path.endswith(".parquet"):
        return pd.read_parquet(path)
    else:
        raise ValueError("Formato no soportado")
