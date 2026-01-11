from db import get_db_engine
from load_dataset_to_db import dataset_loader
import traceback

if __name__ == "__main__":
    try:
        engine = get_db_engine()
        
        if engine:
            print("✅ Conexión a la base de datos establecida.")
            loader = dataset_loader(engine)
            loader.set_file_list()
            loader.process_and_upload()
        else:
            print("No se pudo establecer la conexión a la base de datos.")
            
    except Exception as e:
        print(f"\n❌ Ocurrió un error en el proceso principal:")
        print(traceback.format_exc())