def clean_column_names(df):
    """
    Limpia los nombres de las columnas del DataFrame.
    Elimina espacios y convierte a minúsculas.
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    print("Columnas limpiadas:", df.columns.tolist())
    return df

def drop_nulls(df):
    """
    Elimina filas con valores nulos en el DataFrame.
    """
    df = df.dropna()
    return df

def filter_positive_values(df):
    """
    Elimina filas donde la columna cantidad o precio_unitario es <= 0.
    """
    required_columns = ['cantidad', 'precio_unitario']
    
    for column in required_columns:
        if column not in df.columns:
            raise ValueError(f"La columna '{column}' no existe en el DataFrame.")
    
    # Filtrar ambas columnas: cantidad > 0 Y precio_unitario > 0
    df = df[(df['cantidad'] > 0) & (df['precio_unitario'] > 0)]
    return df

def remove_empty_names(df):
    """
    Elimina filas donde el nombre del vendedor es vacío o nulo.
    """
    if 'vendedor' not in df.columns:
        raise ValueError("La columna 'vendedor' no existe en el DataFrame.")

    df = df[df['vendedor'].notna() & (df['vendedor'] != "")]
    return df

def normalize_product_names(df):
    """
    Normaliza los nombres de los productos, eliminando espacios y convirtiendo a minúsculas y sacando tildes.
    """
    if 'producto' not in df.columns:
        raise ValueError("La columna 'producto' no existe en el DataFrame.")

    df['producto'] = df['producto'].str.strip().str.lower()
    df['producto'] = df['producto'].str.normalize('NFD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return df