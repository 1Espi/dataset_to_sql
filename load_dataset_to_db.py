import pandas as pd
from sqlalchemy.orm import sessionmaker
from file_upload import get_target_files
from tabulate import tabulate
import traceback
from pathlib import Path
import db

class dataset_loader:
    def __init__(self, db_engine):
        self.engine = db_engine
        self.Session = sessionmaker(bind=self.engine)
        self.file_list = []

    def set_file_list(self):
        self.file_list = get_target_files()

    def process_and_upload(self):
        # 1. Mostrar Previsualización
        print("\n--- RESUMEN DE TABLAS A CARGAR ---")
        existing_tables = []
        skip_question = False
        valid_dataframes = [] # Guardamos (df, nombre_tabla) para no leer el archivo dos veces

        for file in self.file_list:
            path_completo = file[0]
            nombre_tabla = Path(path_completo).stem
            
            if db.table_exists(self.engine, nombre_tabla) and not skip_question:
                existing_tables.append(nombre_tabla)
                input_msg = input("\nLa tabla '{}' ya existe. ¿Deseas eliminarla? (a/y/n): ".format(nombre_tabla)).strip().lower()
                if input_msg in ['y', 'a']:
                    db.delete_table(self.engine, nombre_tabla)
                elif input_msg == 'n':
                    continue
                elif input_msg == 'a':
                    skip_question = True
            
            df = df_from_file(file) # Tu función que lee el archivo
            
            if df is not None:
                print(f"\n📊 Tabla destino: {nombre_tabla}")
                
                # Lógica para limitar la previsualización a 5 columnas
                if len(df.columns) > 5:
                    # Seleccionamos solo las primeras 5 columnas para el tabulate
                    df_preview = df.iloc[:, :5]
                    columnas_restantes = df.columns[5:].tolist()
                    
                    print(tabulate(df_preview.head(5), headers='keys', tablefmt='psql'))
                    print(f"🔹 Y otras ({len(columnas_restantes)}) columnas: {', '.join(columnas_restantes)}")
                    print("🔹 Total de filas: {}".format(len(df)))
                else:
                    # Si tiene 5 o menos, se muestra normal
                    print(tabulate(df.head(5), headers='keys', tablefmt='psql'))
                    print("🔹 Total de filas: {}".format(len(df)))
                
                valid_dataframes.append((df, nombre_tabla))

        if not valid_dataframes:
            print("No hay datos válidos para procesar.")
            return

        # 2. Confirmación del usuario
        confirmacion = input(f"\n¿Deseas enviar estas {len(valid_dataframes)} tablas a la base de datos? (y/n): ").strip().lower()

        if confirmacion == 'y' or confirmacion == 'Y':
            print("\n🚀 Iniciando carga...")
            for df, nombre_tabla in valid_dataframes:
                try:
                    # Usamos to_sql para la carga directa
                    df.to_sql(nombre_tabla, con=self.engine, if_exists='replace', index=False)
                    print(f"✅ Tabla '{nombre_tabla}' cargada exitosamente.")
                except Exception as e:
                    print(f"❌ Error al cargar '{nombre_tabla}': {e}")
            print("\n✨ Proceso finalizado.")
        else:
            print("\n🚫 Carga cancelada por el usuario.")
        
def df_from_file(file_path: tuple[str, str]) -> str:
    try:
        df = None
        if file_path[1] == '.csv':
            df = pd.read_csv(file_path[0])
        elif file_path[1] in {'.xlsx', '.xls'}:
            df = pd.read_excel(file_path[0])
        elif file_path[1] == '.json':
            df = pd.read_json(file_path[0])
        elif file_path[1] == '.txt':
            df = pd.read_csv(file_path[0], delimiter='\t')
        elif file_path[1] == '.parquet':
            df = pd.read_parquet(file_path[0])
        elif file_path[1] == '.xml':
            df = pd.read_xml(file_path[0])
        else:
            pass
            
        return df
    except Exception as e:
        print(f"\n❌ Ocurrió un error al leer el archivo {file_path}:")
        print(traceback.format_exc()) 
        return None