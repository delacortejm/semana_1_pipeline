from pipeline.load import load_csv, load_data
from pipeline.save import save_csv

# Cargar datos (ruta corregida)
df = load_data("data/input.csv")
print(df.head())

# Guardar datos procesados
save_csv(df, "data/output/clean.csv")
