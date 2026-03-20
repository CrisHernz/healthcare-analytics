"""
main.py - Orquestador del Pipeline Completo
Ejecución: Notebook (Limpieza) → ETL → Bot (en paralelo)
"""

import os
import sys
import threading
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

from scripts.model_factory import create_star_schema
from scripts.db_connector import PostgresLoader
from scripts.data_cleaner import execute_notebook

# Cargar variables de entorno desde .env
load_dotenv('config/.env')

# Importar función de notificación de bot
try:
    from scripts.telegram_bot import notify_etl_completion
    BOT_AVAILABLE = True
except ImportError:
    BOT_AVAILABLE = False
    print("[WARN] Bot de Telegram no disponible (pyTelegramBotAPI no instalado)")



def main():
    start_time = datetime.now()
    print("\n" + "=" * 80)
    print("  HEALTHCARE ANALYTICS - SISTEMA COMPLETO")
    print("=" * 80 + "\n")
    
    try:
        # ===== FASE 1: EJECUTAR NOTEBOOK (LIMPIEZA) =====
        print("Fase 1: Ejecutando notebook de limpieza...\n")
        
        if not execute_notebook():
            raise Exception("Error al ejecutar el notebook de limpieza")
        
        # ===== FASE 2: CARGAR CSV LIMPIO =====
        print("Fase 2: Cargando datos limpios...")
        df_clean = pd.read_csv('healthcare_dataset_cleaned.csv')
        print(f"   Cargados: {len(df_clean)} registros")
        total_records = len(df_clean)
        
        # ===== FASE 3: TRANSFORMA A MODELO ESTRELLA =====
        print("\nFase 3: Generando modelo estrella...")
        star_schema = create_star_schema(df_clean)
        for table_name, df_table in star_schema.items():
            print(f"   {table_name}: {len(df_table)} registros")
        
        # ===== FASE 4: CARGA A POSTGRESQL =====
        print("\nFase 4: Conectando a PostgreSQL...")
        loader = PostgresLoader(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 5432)),
            database=os.getenv('DB_NAME', 'healthcare_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', '')
        )
        
        if not loader.test_connection():
            raise Exception("No se pudo conectar a PostgreSQL")
        print("   Conexión exitosa")
        
        print("Fase 5: Creando schema...")
        schema_file = os.getenv('SCHEMA_FILE', 'config/schema.sql')
        if not loader.create_schema(schema_file):
            raise Exception("Error al crear schema")
        print("   Schema creado")
        
        print("Fase 6: Cargando datos (Truncate & Load)...")
        if not loader.truncate_and_load(star_schema):
            raise Exception("Error en carga")
        print("   Datos cargados")
        
        loader.close()
        
        # ===== FASE 7: NOTIFICACIÓN Y BOT =====
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 80)
        print("[OK] PIPELINE COMPLETADO")
        print("=" * 80)
        print(f"  Registros procesados: {total_records}")
        print(f"  Duración: {duration:.2f}s")
        print(f"  Timestamp: {end_time.isoformat()}")
        print("=" * 80 + "\n")
        
        # Notificar por Telegram
        if BOT_AVAILABLE:
            try:
                metrics = {
                    'total_records': total_records,
                    'execution_time': round(duration, 2),
                    'load_timestamp': end_time.isoformat()
                }
                if notify_etl_completion(metrics):
                    print("[OK] Notificacion enviada a Telegram\n")
            except Exception as e:
                print(f"[WARN] No se pudo enviar notificacion: {e}\n")
        
        # Iniciar Bot en thread separado
        print("Fase 7: Iniciando Bot de Telegram...\n")
        try:
            from scripts.telegram_bot import main as start_bot
            
            bot_thread = threading.Thread(
                target=start_bot,
                daemon=False
            )
            bot_thread.start()
            bot_thread.join()
        
        except KeyboardInterrupt:
            print("\n[OK] Sistema detenido por usuario\n")
            return 0
        except Exception as e:
            print(f"[WARN] Error en Bot: {e}\n")
            return 0
        
        return 0
    
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 80)
        print("[ERROR] ERROR EN PIPELINE")
        print("=" * 80)
        print(f"  {str(e)}")
        print(f"  Duración: {duration:.2f}s")
        print("=" * 80 + "\n")
        
        return 1


if __name__ == '__main__':
    sys.exit(main())
