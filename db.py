from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv
import os
import traceback

def get_db_engine():
    try:
        cwd = os.getcwd()
        env_path = cwd + "/.env"

        load_dotenv(env_path)

        url = os.getenv("DATABASE_URL")

        engine = create_engine(url)
        return engine
    
    except Exception as e:
        print(f"\n❌ Ocurrió un error al obtener la conexión a la base de datos:")
        print(traceback.format_exc()) 
        return None

def table_exists(engine, table_name: str) -> bool:
    # El inspector es la forma más fiable y compatible
    inspector = inspect(engine)
    return inspector.has_table(table_name)
    

def delete_table(engine, table_name: str) -> bool:
    try:
        with engine.begin() as connection: # .begin() hace el commit automático
            connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            return True
    except Exception as e:
        print(f"\n❌ Error al eliminar tabla {table_name}: {e}")
        return False