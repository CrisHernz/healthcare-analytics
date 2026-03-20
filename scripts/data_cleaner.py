"""
data_cleaner.py - Ejecutor del Notebook de Limpieza
Ejecuta solucion.ipynb y genera healthcare_dataset_cleaned.csv
"""

import subprocess
import sys
import os
from pathlib import Path


def execute_notebook():
    """
    Ejecuta el notebook solucion.ipynb usando nbconvert
    Genera: healthcare_dataset_cleaned.csv
    """
    print("\n" + "=" * 80)
    print("  DATA CLEANING - NOTEBOOK EXECUTION")
    print("=" * 80 + "\n")
    
    try:
        # Verificar que el notebook existe
        notebook_path = Path('solucion.ipynb')
        if not notebook_path.exists():
            raise FileNotFoundError(f"No se encontró: {notebook_path}")
        
        print(f"Ejecutando: {notebook_path}\n")
        
        # Ejecutar el notebook con nbconvert
        result = subprocess.run(
            [sys.executable, '-m', 'nbconvert', 
             '--to', 'notebook', 
             '--execute', 
             '--ExecutePreprocessor.timeout=600',
             '--output', 'solucion.ipynb',
             str(notebook_path)],
            capture_output=False,
            text=True
        )
        
        if result.returncode != 0:
            raise Exception(f"Error al ejecutar notebook (código {result.returncode})")
        
        # Verificar que se generó el archivo limpio
        cleaned_csv = Path('healthcare_dataset_cleaned.csv')
        if not cleaned_csv.exists():
            raise FileNotFoundError("No se generó healthcare_dataset_cleaned.csv")
        
        print("\n" + "=" * 80)
        print("[OK] NOTEBOOK COMPLETADO")
        print("=" * 80)
        print(f"  Archivo generado: healthcare_dataset_cleaned.csv")
        print("=" * 80 + "\n")
        
        return True
    
    except Exception as e:
        print(f"\n[ERROR] Error ejecutando notebook: {str(e)}\n")
        return False


if __name__ == '__main__':
    success = execute_notebook()
    sys.exit(0 if success else 1)
