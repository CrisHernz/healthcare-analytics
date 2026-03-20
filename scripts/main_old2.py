"""
main.py - Orquestador del Pipeline Completo
Ejecución: Bot (thread) → Esperar chat_id → ETL → Notificar → Bot activo
"""

import os
import sys
import threading
import time
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

from scripts.model_factory import create_star_schema
from scripts.db_connector import PostgresLoader
from scripts.data_cleaner import execute_notebook

# Cargar variables de entorno desde .env
load_dotenv('config/.env')

# Importar funciones de bot
try:
    from scripts.telegram_bot import notify_etl_completion, main as start_bot, USER_SESSIONS
    BOT_AVAILABLE = True
except ImportError:
    BOT_AVAILABLE = False
    print("[WARN] Bot de Telegram no disponible (pyTelegramBotAPI no instalado)")


def wait_for_chat_id(timeout=120):
    """
    Espera a que el usuario abra un chat con el bot (timeout en segundos)
    Retorna el chat_id del primer usuario que se conecte, o None si timeout
    """
    print(f"\n⏳ Esperando que abras el chat en Telegram → /start")
    print(f"   Timeout en {timeout} segundos...\n")
    
    start_wait = time.time()
    while time.time() - start_wait < timeout:
        # Si hay algún usuario en USER_SESSIONS, obtener su chat_id
        if USER_SESSIONS:
            first_user_id = list(USER_SESSIONS.keys())[0]
            chat_id = USER_SESSIONS[first_user_id]['chat_id']
            print(f"[OK] Chat abierto - User {first_user_id}, Chat ID: {chat_id}\n")
            return chat_id
        
        time.sleep(1)  # Verificar cada segundo
    
    print("[WARN] Timeout esperando chat. Continuando sin notificación inicial...\n")
    return None


def main():
    start_time = datetime.now()
    chat_id = None  # Variable para almacenar el chat_id
    
    print("\n" + "=" * 80)
    print("  HEALTHCARE ANALYTICS - SISTEMA COMPLETO v2.1")
    print("=" * 80 + "\n")
    
    try:
        # ===== FASE 0: INICIAR BOT EN THREAD =====
        print("Fase 0: Inicializando Bot de Telegram...\n")
        
        if BOT_AVAILABLE:
            # Lanzar bot en thread (daemon=True para que no bloquee)
            bot_thread = threading.Thread(target=start_bot, daemon=True)
            bot_thread.start()
            
            # Esperar a que el usuario abra el chat (máx 120 segundos)
            chat_id = wait_for_chat_id(timeout=120)
        
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
        
        # ===== FASE 7: NOTIFICACIÓN POR TELEGRAM =====
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 80)
        print("[OK] PIPELINE COMPLETADO")
        print("=" * 80)
        print(f"  Registros procesados: {total_records}")
        print(f"  Duración: {duration:.2f}s")
        print(f"  Timestamp: {end_time.isoformat()}")
        print("=" * 80 + "\n")
        
        # Notificar por Telegram (solo si hay chat_id)
        if BOT_AVAILABLE and chat_id:
            try:
                metrics = {
                    'total_records': total_records,
                    'execution_time': round(duration, 2),
                    'load_timestamp': end_time.isoformat(),
                    'chat_id': chat_id  # Pasar el chat_id para notificar
                }
                if notify_etl_completion(metrics):
                    print("[OK] Notificacion enviada a Telegram\n")
            except Exception as e:
                print(f"[WARN] No se pudo enviar notificacion: {e}\n")
        elif not chat_id and BOT_AVAILABLE:
            print("[WARN] No hay chat_id disponible. Usuario puede consultar con /status\n")
        
        # ===== FASE 8: BOT CONTINÚA ACTIVO =====
        print("=" * 80)
        print("Fase 8: Bot de Telegram ACTIVO - Esperando consultas...")
        print("=" * 80)
        print("\nComandos disponibles en Telegram:")
        print("  /start  - Menú principal")
        print("  /status - Estado del sistema")
        print("  /help   - Información de comandos")
        print("\nPresiona CTRL+C para detener el sistema\n")
        
        # Mantener el proceso activo (el bot sigue en su thread daemon)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[OK] Sistema detenido por usuario\n")
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
