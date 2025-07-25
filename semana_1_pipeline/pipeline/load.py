import pandas as pd

def load_data(path: str) -> pd.DataFrame:
    if path.endswith(".csv"):
        df = pd.read_csv(path)
    elif path.endswith(".parquet"):
        df = pd.read_parquet(path)
    else:
        raise ValueError("Formato no soportado")
    
    print(f"Se cargaron {len(df)} registros desde {path}")
    return df
