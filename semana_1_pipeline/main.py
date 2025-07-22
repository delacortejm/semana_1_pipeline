from pipeline.load import load_csv
from pipeline.save import save_csv

# Cargar datos (ruta corregida)
df = load_csv("data/input.csv")
print(df.head())

# Guardar datos procesados
save_csv(df, "data/output/clean.csv")

# from pipeline.load import load_csv
# from pipeline.save import save_csv

# df = load_csv("data/input.csv")
# print(df.head())

# save_csv(df, "data/output/clean.csv")
