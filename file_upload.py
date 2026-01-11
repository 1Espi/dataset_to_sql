import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
import traceback

def get_target_files() -> list[tuple[str, str]]:
    try:
        root = tk.Tk()
        root.withdraw()

        valid_extensions = {'.csv', '.xlsx', '.xls', '.json', '.txt', '.parquet', '.xml'}
        choice = messagebox.askyesnocancel(
            "Selección de origen", 
            "¿Deseas seleccionar una CARPETA completa? \n(Presiona 'No' para elegir archivos específicos)"
        )

        temp_files = [] # Usamos una lista temporal de rutas

        if choice is True:
            folder_path = filedialog.askdirectory(title="Selecciona la carpeta")
            if folder_path:
                for file in Path(folder_path).iterdir():
                    if file.is_file():
                        temp_files.append(str(file))

        elif choice is False:
            files = filedialog.askopenfilenames(
                title="Selecciona uno o más archivos",
                filetypes=[("Archivos de datos", "*.csv *.xlsx *.xls *.json *.txt *.parquet *.xml")]
            )
            temp_files = list(files)

        elif choice is None:
            root.destroy()
            return []

        # Transformación a lista de tuplas [(path, ext), ...] con filtros aplicados
        selected_files = [
            (str(f), Path(f).suffix.lower()) 
            for f in temp_files 
            if Path(f).suffix.lower() in valid_extensions and not Path(f).name.startswith('~$')
        ]

        if not selected_files and choice is not None:
            messagebox.showwarning("Advertencia", "No se seleccionaron archivos válidos.")
            root.destroy()
            return get_target_files()

        root.destroy()
        return selected_files

    except Exception as e:
        print(f"\n❌ Ocurrió un error en get_target_files:")
        print(traceback.format_exc()) 
        messagebox.showerror("Error Crítico", f"Detalle del error: {e}")
        return []

if __name__ == "__main__":
    lista_final = get_target_files()
    
    if lista_final:
        print("\n--- Archivos listos para procesar ---")
        for ruta, ext in lista_final:
            print(f"Procesando: {ruta} | Extensión: {ext}")