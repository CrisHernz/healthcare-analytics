"""
telegram_bot.py - Bot de Telegram para notificaciones y consultas de Healthcare ETL
Bot multi-usuario con ejecución de ETL bajo demanda
"""

import os
import sys
import psycopg2
from psycopg2 import OperationalError, ProgrammingError
from dotenv import load_dotenv
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from contextlib import contextmanager
import threading
from datetime import datetime
from pathlib import Path
import time

# Cargar variables de entorno (desde ruta absoluta)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, '..', 'config', '.env')
load_dotenv(ENV_PATH)

print(f"Cargando .env desde: {ENV_PATH}")
print(f"Archivo existe: {os.path.exists(ENV_PATH)}")

# Variables de configuración
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')  # Para compatibilidad (ya no es obligatorio)

# Validar token
if not TELEGRAM_TOKEN:
    print("ERROR: TELEGRAM_TOKEN no configurado en .env")
    raise ValueError("TELEGRAM_TOKEN no encontrado")

print(f"Token: {TELEGRAM_TOKEN[:20]}...")
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', 5432))
DB_NAME = os.getenv('DB_NAME', 'healthcare_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

# ============================================================================
# GESTIÓN DE SESIONES Y EJECUCIONES
# ============================================================================

# Diccionario para rastrear sesiones activas: {user_id: {'chat_id': ..., 'timestamp': ..., 'status': ...}}
USER_SESSIONS = {}

# Diccionario para rastrear ejecuciones de ETL: {user_id: {'chat_id': ..., 'start_time': ..., 'status': ...}}
ETL_EXECUTIONS = {}

# Lock para acceso thread-safe
SESSION_LOCK = threading.Lock()

def create_user_session(user_id, chat_id):
    """Crea una nueva sesión de usuario"""
    with SESSION_LOCK:
        USER_SESSIONS[user_id] = {
            'chat_id': chat_id,
            'timestamp': datetime.now(),
            'status': 'active'
        }
    print(f"[OK] Sesion creada para usuario {user_id}")

def get_user_session(user_id):
    """Obtiene la sesión de un usuario"""
    with SESSION_LOCK:
        return USER_SESSIONS.get(user_id)

def remove_user_session(user_id):
    """Elimina la sesión de un usuario"""
    with SESSION_LOCK:
        if user_id in USER_SESSIONS:
            del USER_SESSIONS[user_id]
            print(f"[OK] Sesion eliminada para usuario {user_id}")

# Inicializar bot
try:
    bot = telebot.TeleBot(TELEGRAM_TOKEN)
    print("[OK] Bot de Telegram inicializado correctamente")
except Exception as e:
    print(f"ERROR al inicializar bot: {e}")
    raise



@contextmanager
def get_db_connection():
    """
    Context manager para manejar conexiones a PostgreSQL
    Garantiza que la conexión se cierre correctamente
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            client_encoding='LATIN1'
        )
        yield conn
    except Exception as e:
        print(f"ERROR de conexioBD: {e}")
        raise
    finally:
        if conn:
            conn.close()


def execute_query(query: str, fetch_all: bool = True):
    """
    Ejecuta una query a la base de datos de forma segura
    
    Args:
        query: Comando SQL a ejecutar
        fetch_all: Si True usa fetchall(), si False usa fetchone()
    
    Returns:
        Resultados de la query
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            
            if fetch_all:
                return cursor.fetchall()
            else:
                return cursor.fetchone()
    except ProgrammingError as e:
        print(f"ERRORn query SQL: {e}")
        return None
    finally:
        if cursor:
            cursor.close()


def escape_html(text: str) -> str:
    """
    Escapa caracteres especiales de HTML para que Telegram pueda parsear correctamente
    Reemplaza caracteres especiales con sus equivalentes escapados
    """
    if text is None:
        return ""
    
    text = str(text)
    # Escapes necesarios para HTML en Telegram
    replacements = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
    }
    
    for char, escape in replacements.items():
        text = text.replace(char, escape)
    
    return text


def notify_etl_completion(metrics_dict: dict, chat_id: str = None) -> bool:
    """
    Notifica al usuario por Telegram que el ETL completó exitosamente
    Ahora soporta chat_id dinámico (para múltiples usuarios)
    
    Args:
        metrics_dict: Dict con las métricas:
            - total_records: int
            - execution_time: float
            - load_timestamp: str (ISO format)
        chat_id: ID del chat a notificar (dinámico). Si no se proporciona, usa TELEGRAM_CHAT_ID
    
    Returns:
        True si se envió correctamente, False si hubo error
    """
    try:
        # Si no se proporciona chat_id, usar el global (compatibilidad con versión anterior)
        target_chat_id = chat_id or TELEGRAM_CHAT_ID
        
        if not target_chat_id:
            print("ERROR: No hay chat_id disponible para notificacion")
            return False
        
        total = metrics_dict.get('total_records', 0)
        time_taken = metrics_dict.get('execution_time', 0)
        timestamp = metrics_dict.get('load_timestamp', 'N/A')
        
        # Mensaje sin caracteres especiales problemáticos
        message = (
            "NOTIFICACION: ETL COMPLETADO\n\n"
            "Resumen de Ejecucion:\n"
            "========================\n"
            f"Registros Procesados: {total:,}\n"
            f"Tiempo de Ejecucion: {time_taken}s\n"
            f"Fecha/Hora de Carga: {timestamp}\n"
            "========================\n\n"
        )
        
        bot.send_message(
            target_chat_id,
            message,
            parse_mode='HTML'
        )
        print(f"[OK] Notificacion enviada a chat {target_chat_id}")
        return True
    except Exception as e:
        print(f"ERROR al enviar notificacio {e}")
        return False


def execute_etl_background(user_id: int, chat_id: int):
    """
    Ejecuta el ETL en un thread separado para no bloquear el bot
    Llamada directa a main() del orquestador
    
    Args:
        user_id: ID del usuario que solicitó la ejecución
        chat_id: Chat ID para enviar notificaciones
    """
    from scripts.main import main as execute_etl
    
    print(f"\n{'='*70}")
    print(f"[ETL] INICIANDO ETL POR SOLICITUD DEL USUARIO {user_id}")
    print(f"{'='*70}\n")
    
    try:
        # Registrar ejecución
        with SESSION_LOCK:
            ETL_EXECUTIONS[user_id] = {
                'chat_id': chat_id,
                'start_time': datetime.now(),
                'status': 'ejecutando'
            }
        
        # Enviar mensaje de inicio
        bot.send_message(
            chat_id,
            "⏳ Iniciando ejecución del ETL...\nEsto puede tomar algunos minutos.",
            parse_mode='HTML'
        )
        
        # Ejecutar ETL mediante llamada directa a main()
        exit_code = execute_etl()
        
        if exit_code == 0:
            # Éxito
            message = "✅ ETL COMPLETADO EXITOSAMENTE\n\n"
            message += "Verifica el menu de opciones para consultar los datos cargados."
            
            bot.send_message(
                chat_id,
                message,
                parse_mode='HTML'
            )
            print(f"[ETL] ETL completado para usuario {user_id}")
        else:
            # Error
            error_msg = f"❌ Error durante la ejecución del ETL (código {exit_code})"
            bot.send_message(
                chat_id,
                error_msg,
                parse_mode='HTML'
            )
            print(f"[ETL] Error en ETL para usuario {user_id}")
        
        # Actualizar estado
        with SESSION_LOCK:
            if user_id in ETL_EXECUTIONS:
                ETL_EXECUTIONS[user_id]['status'] = 'completado'
        
    except Exception as e:
        print(f"[ETL] Excepción en ETL para usuario {user_id}: {e}")
        import traceback
        traceback.print_exc()
        
        try:
            bot.send_message(
                chat_id,
                f"❌ Error: {str(e)}",
                parse_mode='HTML'
            )
        except:
            pass


@bot.message_handler(commands=['start'])
def send_welcome(message):
    """
    Handler para comando /start
    Envía menú interactivo con opciones - ahora con soporte multi-usuario
    """
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    print(f"[START] Comando /start recibido de usuario {user_id} (chat {chat_id})")
    
    # Crear sesión del usuario
    create_user_session(user_id, chat_id)
    
    welcome_text = (
        "🏥 <b>Bienvenido al Bot Healthcare Analytics</b>\n\n"
        "Tu sesión ha sido iniciada correctamente y el sistema está preparando los datos.\n"
        "Por favor espera el mensaje de confirmación antes de continuar.\n\n"
    )
    try:
        print(f"[START] Enviando mensaje a chat {chat_id}...")
        result = bot.send_message(
            chat_id,
            welcome_text,
            parse_mode='HTML'
        )
        print(f"[OK] Menu enviado correctamente a {chat_id}. Message ID: {result.message_id}")
    except Exception as e:
        print(f"[ERROR] En /start: {str(e)}")
        import traceback
        traceback.print_exc()
        try:
            bot.send_message(
                chat_id,
                f"Error: {str(e)}"
            )
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "etl_start")
def ejecutar_etl_inline(call):
    """
    Callback para ejecutar ETL desde botón inline en /start
    """
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    print(f"[ETL] Callback ETL solicitado por usuario {user_id}")
    
    # Verificar si ya hay una ejecución en progreso
    with SESSION_LOCK:
        if user_id in ETL_EXECUTIONS and ETL_EXECUTIONS[user_id]['status'] == 'ejecutando':
            bot.answer_callback_query(call.id, "Ya hay un ETL en ejecucion", show_alert=True)
            return
    
    # Confirmar ejecución
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("Si, ejecutar", callback_data="confirmar_etl"))
    markup.add(InlineKeyboardButton("Cancelar", callback_data="cancelar_etl"))
    
    bot.send_message(
        chat_id,
        "Confirmas que deseas ejecutar el ETL ahora?\n\nEsto puede tomar varios minutos...",
        reply_markup=markup
    )
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data == "reporte_ejecutivo")
def reporte_ejecutivo(call):
    """
    Callback para Reporte Ejecutivo
    Muestra: Total facturación, admisiones, promedio de días
    """
    print(f"🔔 Callback reporte_ejecutivo activado por chat_id: {call.message.chat.id}")
    
    try:
        query = """
                SELECT 
                    COUNT(*) AS total_admisiones,
                    COUNT(DISTINCT fa.patient_id) AS pacientes_unicos,
                    COALESCE(SUM(fa.billing_amount), 0) AS total_facturacion,
                    ROUND(AVG(fa.billing_amount)::numeric, 2) AS costo_promedio,
                    ROUND(AVG(fa.hospitalization_days)::numeric, 2) AS promedio_hospitalizacion,
                    MAX(fa.hospitalization_days) AS max_hospitalizacion
                FROM fact_admisiones fa;
                """
        print(f"Ejecutando query: {query[:50]}...")
        result = execute_query(query, fetch_all=False)
        print(f"Resultado: {result}")
        
        if result:
            total_admisiones = result[0]
            pacientes_unicos = result[1]
            total_facturacion = result[2]
            costo_promedio = result[3]
            promedio_hospitalizacion = result[4]
            max_hospitalizacion = result[5]

            response = (
                "📊 REPORTE EJECUTIVO\n\n"
                f"• Total de Admisiones: {total_admisiones:,}\n"
                f"• Total de Pacientes: {pacientes_unicos:,}\n"
                f"• Facturación Total: ${total_facturacion:,.2f}\n"
                f"• Costo Promedio por Paciente: ${costo_promedio:,.2f}\n"
                f"• Días de Hospitalización promedio: {promedio_hospitalizacion} días\n"
                f"• Máximo de Días de Hospitalización: {max_hospitalizacion} días\n\n"
            )
        else:
            response = "Error al consultar la base de datos"
        
        bot.send_message(
            call.message.chat.id,
            response,
            parse_mode='HTML'
        )
        print("✅ Respuesta enviada")
        
    except Exception as e:
        print(f"❌ Error en reporte_ejecutivo: {e}")
        import traceback
        traceback.print_exc()
        bot.send_message(
            call.message.chat.id,
            f"Error: {str(e)}"
        )
        bot.answer_callback_query(call.id, "Error al procesar")


@bot.callback_query_handler(func=lambda call: call.data == "analisis_riesgos")
def analisis_riesgos(call):
    """
    Callback para Análisis de Riesgos
    Calcula porcentaje de resultados Abnormal, Normal e Inconclusive
    """
    print(f"🔔 Callback analisis_riesgos activado por chat_id: {call.message.chat.id}")
    
    try:
        query = """
            SELECT 
                COUNT(*) AS total_pruebas,
                COALESCE(SUM(CASE WHEN test_id = 3 THEN 1 ELSE 0 END), 0) AS pruebas_abnormal,
                COALESCE(SUM(CASE WHEN test_id = 1 THEN 1 ELSE 0 END), 0) AS pruebas_normal,
                COALESCE(SUM(CASE WHEN test_id = 2 THEN 1 ELSE 0 END), 0) AS pruebas_inconclusive,
                CASE WHEN COUNT(*) > 0 THEN ROUND(100.0 * SUM(CASE WHEN test_id = 3 THEN 1 ELSE 0 END) / COUNT(*), 2) ELSE 0 END AS pct_abnormal,
                CASE WHEN COUNT(*) > 0 THEN ROUND(100.0 * SUM(CASE WHEN test_id = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) ELSE 0 END AS pct_normal,
                CASE WHEN COUNT(*) > 0 THEN ROUND(100.0 * SUM(CASE WHEN test_id = 2 THEN 1 ELSE 0 END) / COUNT(*), 2) ELSE 0 END AS pct_inconclusive
            FROM fact_admisiones;
        """
        print(f"Ejecutando query analisis_riesgos...")
        results = execute_query(query, fetch_all=True)
        print(f"Resultados: {results}")
        
        if results and len(results) > 0:
            # results es una lista de tuplas, tomamos la primera fila
            row = results[0]
            total_pruebas = row[0]
            pruebas_abnormal = row[1] if row[1] is not None else 0
            pruebas_normal = row[2] if row[2] is not None else 0
            pruebas_inconclusive = row[3] if row[3] is not None else 0
            pct_abnormal = float(row[4]) if row[4] is not None else 0.0
            pct_normal = float(row[5]) if row[5] is not None else 0.0
            pct_inconclusive = float(row[6]) if row[6] is not None else 0.0

            response = (
                "⚠️ ANÁLISIS DE RIESGOS\n\n"
                f"• Total de Pruebas: {total_pruebas:,}\n"
                f"• Resultados Abnormal: {pruebas_abnormal:,} ({pct_abnormal:.1f}%)\n"
                f"• Resultados Normal: {pruebas_normal:,} ({pct_normal:.1f}%)\n"
                f"• Resultados Inconclusive: {pruebas_inconclusive:,} ({pct_inconclusive:.1f}%)\n\n"
            )

            # Insight narrativo
            if pct_abnormal > 30:
                response += "🔴 Riesgo elevado: Más del 30% de las pruebas son anormales.\n"
            elif pct_abnormal > 15:
                response += "🟠 Riesgo moderado: Entre 15% y 30% de las pruebas son anormales.\n"
            else:
                response += "🟢 Riesgo bajo: Menos del 15% de las pruebas son anormales.\n"

        else:
            response = "Error al consultar la base de datos"
        
        bot.send_message(
            call.message.chat.id,
            response,
            parse_mode='HTML'
        )
        print("✅ Respuesta enviada")
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        print(f"❌ Error en analisis_riesgos: {e}")
        import traceback
        traceback.print_exc()
        bot.send_message(
            call.message.chat.id,
            f"Error: {str(e)}"
        )
        bot.answer_callback_query(call.id, "Error al procesar")



@bot.callback_query_handler(func=lambda call: call.data == "top_aseguradoras")
def top_aseguradoras(call):
    """
    Callback para Top 3 Aseguradoras
    Muestra las 3 aseguradoras que más facturan
    """
    print(f"🔔 Callback top_aseguradoras activado por chat_id: {call.message.chat.id}")
    
    try:
        query = """
        SELECT 
            ds.provider_name,
            SUM(fa.billing_amount) AS total_facturacion,
            COUNT(fa.admission_id) AS total_admisiones
        FROM fact_admisiones fa
        JOIN dim_seguros ds ON fa.insurance_id = ds.insurance_id
        GROUP BY ds.provider_name
        ORDER BY total_facturacion DESC
        LIMIT 3;
        """
        
        print(f"Ejecutando query top_aseguradoras...")
        results = execute_query(query, fetch_all=True)
        print(f"Resultados: {results}")
        
        if results:
            response = "🏆 TOP 3 ASEGURADORAS POR FACTURACIÓN\n\n"
            
            for idx, result in enumerate(results, 1):
                aseguradora = escape_html(str(result[0]))  # ESCAPAR DATOS
                facturacion = result[1]
                admisiones = result[2]
                
                response += f"{idx}. {aseguradora}\n"
                response += f"   Facturación: ${facturacion:,.2f}\n"
                response += f"   Admisiones: {admisiones:,}\n\n"
            
        else:
            response = "Error al consultar la base de datos"
        
        bot.send_message(
            call.message.chat.id,
            response,
            parse_mode='HTML'
        )
        print("✅ Respuesta enviada")
        
    except Exception as e:
        print(f"❌ Error en top_aseguradoras: {e}")
        import traceback
        traceback.print_exc()
        bot.send_message(
            call.message.chat.id,
            f"Error: {str(e)}"
        )
        bot.answer_callback_query(call.id, "Error al procesar")


@bot.callback_query_handler(func=lambda call: call.data == "ver_dashboard")
def ver_dashboard(call):
    """
    Handler para Ver Dashboard en PowerBI
    Envía la URL del dashboard de PowerBI al usuario
    """
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    print(f"🔔 Callback ver_dashboard activado por chat_id: {chat_id}")
    
    dashboard_url = "https://epnecuador-my.sharepoint.com/:u:/g/personal/cristian_hernandez_epn_edu_ec/IQC6GAHeCc8HR7BCJs9VLRpvAfl54uKBA_f9qGWptIEBHKE?e=TGmCrY"
    
    # Enviar imagen del dashboard
    try:
        with open('Capturas/Dashboard.png', 'rb') as img:
            bot.send_photo(
                chat_id,
                img,
                caption=(
                    "<b>📊 DASHBOARD POWERBI</b>\n\n"
                    "Haz click en el enlace para acceder al dashboard interactivo:\n\n"
                    f'<a href="{dashboard_url}">Abrir Dashboard</a>'
                ),
                parse_mode='HTML'
            )
        print("✅ Imagen del dashboard enviada")
    except FileNotFoundError:
        print("⚠️ Archivo de imagen no encontrado, enviando mensaje alternativo")
        response = (
            "<b>📊 DASHBOARD POWERBI</b>\n\n"
            "Haz click en el enlace para acceder al dashboard interactivo:\n\n"
            f'<a href="{dashboard_url}">Abrir Dashboard</a>\n\n'
            "En el dashboard encontrarás:\n"
            "• Volumen y estacionalidad de admisiones\n"
            "• Top hospitales por facturación y desglose por condición médica\n"
            "• Tiempo promedio de hospitalización (LOS)\n"
            "• Resultados anormales y análisis de riesgos\n"
            "• KPI de costo promedio por paciente\n"
        )
        bot.send_message(
            chat_id,
            response,
            parse_mode='HTML',
            disable_web_page_preview=False
        )
    
    bot.answer_callback_query(call.id, "Dashboard abierto")


@bot.message_handler(commands=['help'])
def send_help(message):
    """
    Handler para comando /help
    Muestra instrucciones de uso del bot multi-usuario
    """
    help_text = (
        "COMANDOS DISPONIBLES:\n\n"
        "/start - Mostrar menu principal\n"
        "/ejecutar_etl - Ejecutar ETL bajo demanda\n"
        "/status - Estado de la base de datos\n"
        "/help - Mostrar esta ayuda\n\n"
        "CARACTERISTICAS:\n\n"
        "Multi-usuario: Cada dispositivo/usuario tiene su propia sesion\n"
        "ETL bajo demanda: Ejecuta el proceso cuando lo necesites\n"
        "Consultas en tiempo real: Datos actualizados al instante\n\n"
        "OPCIONES DEL MENU:\n\n"
        "Reporte Ejecutivo:\n"
        "  - Total de admisiones\n"
        "  - Total de facturacion\n"
        "  - Promedio de hospitalizacion\n\n"
        "Analisis de Riesgos:\n"
        "  - Porcentaje de resultados anormales\n"
        "  - Distribucion de pruebas\n\n"
        "Top 3 Aseguradoras:\n"
        "  - Ranking por facturacion\n"
        "  - Cantidad de admisiones por aseguradora"
    )
    
    bot.send_message(
        message.chat.id,
        help_text,
        parse_mode='HTML'
    )


@bot.message_handler(commands=['status'])
def send_status(message):
    """
    Handler para comando /status
    Verifica estado de la conexión a BD
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM fact_admisiones) as fact_count,
                (SELECT COUNT(*) FROM dim_pacientes) as patient_count,
                (SELECT COUNT(*) FROM dim_doctores) as doctor_count,
                (SELECT COUNT(*) FROM dim_hospitales) as hospital_count,
                (SELECT COUNT(*) FROM dim_seguros) as insurance_count;
            """)
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                response = (
                    "ESTADO DE LA BASE DE DATOS\n\n"
                    f"Estado: CONECTADO\n"

                    "REGISTROS CARGADOS:\n"
                    f"Tabla de Hechos: {result[0]:,}\n"
                    f"Pacientes: {result[1]:,}\n"
                    f"Doctores: {result[2]:,}\n"
                    f"Hospitales: {result[3]:,}\n"
                    f"Aseguradoras: {result[4]:,}"
                )
            else:
                response = "No se pudo obtener informacion"
    except Exception as e:
        response = f"Error de conexion:\n{str(e)}"
    
    bot.send_message(
        message.chat.id,
        response,
        parse_mode='HTML'
    )


@bot.message_handler(commands=['ejecutar_etl', 'ejecutar'])
def ejecutar_etl(message):
    """
    Handler para comando /ejecutar_etl
    Permite al usuario ejecutar el ETL bajo demanda
    """
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    print(f"\n📌 Comando /ejecutar_etl solicitado por usuario {user_id} (chat {chat_id})")
    
    # Crear sesión del usuario
    create_user_session(user_id, chat_id)
    
    # Verificar si ya hay una ejecución en progreso
    with SESSION_LOCK:
        if user_id in ETL_EXECUTIONS and ETL_EXECUTIONS[user_id]['status'] == 'ejecutando':
            bot.send_message(
                chat_id,
                "⚠️ Ya hay un ETL en ejecución para tu usuario.\nPor favor espera a que se complete.",
                parse_mode='HTML'
            )
            return
    
    # Mensaje de confirmación
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("Si, ejecutar ETL", callback_data="confirmar_etl"))
    markup.add(InlineKeyboardButton("Cancelar", callback_data="cancelar_etl"))
    
    bot.send_message(
        chat_id,
        "¿Confirmas que deseas ejecutar el ETL ahora?\n\nEsto puede tomar varios minutos...",
        reply_markup=markup,
        parse_mode='HTML'
    )


@bot.callback_query_handler(func=lambda call: call.data == "confirmar_etl")
def confirmar_etl(call):
    """
    Callback para confirmar ejecución de ETL
    """
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    print(f"✅ ETL confirmado por usuario {user_id}")
    
    # Ejecutar ETL en background thread
    etl_thread = threading.Thread(
        target=execute_etl_background,
        args=(user_id, chat_id),
        daemon=False
    )
    etl_thread.start()
    
    # Respuesta al callback
    bot.answer_callback_query(call.id, "Ejecutando ETL en background...")


@bot.callback_query_handler(func=lambda call: call.data == "cancelar_etl")
def cancelar_etl(call):
    """
    Callback para cancelar ejecución de ETL
    """
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    print(f"❌ ETL cancelado por usuario {user_id}")
    
    bot.send_message(
        chat_id,
        "❌ Ejecución de ETL cancelada",
        parse_mode='HTML'
    )
    bot.answer_callback_query(call.id, "Cancelado")


@bot.message_handler(func=lambda message: True)
def handle_any_text(message):
    """
    Handler por defecto para cualquier otro texto
    Muestra el menú interactivo con opción de ejecutar ETL
    """
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    print(f"📨 Mensaje recibido: '{message.text}' de usuario {user_id} (chat {chat_id})")
    
    # Crear sesión del usuario
    create_user_session(user_id, chat_id)
    
    welcome_text = (
        "📋             MENÚ PRINCIPAL      \n\n"
        "Selecciona una de las siguientes opciones:\n"
    )
    
    # Crear teclado inline con botones
    markup = InlineKeyboardMarkup()
    
    # Botón de ejecutar ETL
    markup.add(InlineKeyboardButton(
        "Ejecutar ETL",
        callback_data="ejecutar_etl_inline"
    ))
    
    # Botones de consultas
    markup.add(InlineKeyboardButton(
        "Reporte Ejecutivo",
        callback_data="reporte_ejecutivo"
    ))
    markup.add(InlineKeyboardButton(
        "Analisis de Riesgos",
        callback_data="analisis_riesgos"
    ))
    markup.add(InlineKeyboardButton(
        "Top 3 Aseguradoras",
        callback_data="top_aseguradoras"
    ))
    markup.add(InlineKeyboardButton(
        "📊 Ver Dashboard",
        callback_data="ver_dashboard"
    ))
    
    try:
        bot.send_message(
            chat_id,
            welcome_text,
            reply_markup=markup,
            parse_mode='HTML'
        )
        print("✅ Menu enviado correctamente en response a mensaje")
    except Exception as e:
        print(f"❌ Error enviando menu: {e}")
        import traceback
        traceback.print_exc()


def main():
    """
    Inicia el bot en modo multi-usuario
    Cada dispositivo/usuario tiene su propia sesión
    """
    print("\n" + "="*70)
    print("[BOT] HEALTHCARE ANALYTICS - MODO MULTI-USUARIO")
    print("="*70)
    print(f"\nToken configurado: {TELEGRAM_TOKEN[:20]}...")
    print(f"Sesiones en memoria: Activas")
    print(f"ecuciones ETL: Bajo demanda")
    print(f"\nComandos disponibles:")
    print("   /start - Menu principal")
    print("   /ejecutar_etl - Ejecutar ETL bajo demanda")
    print("   /help - Ayuda y comandos")
    print("   /status - Estado de la BD")
    print(f"\n{'='*70}")
    print("Escuchando mensajes de multiples usuarios...")
    print("="*70 + "\n")
    
    try:
        bot.infinity_polling()
    except KeyboardInterrupt:
        print("\n[STOPPED] Bot detenido")
        print(f"[INFO] Sesiones activas: {len(USER_SESSIONS)}")
        print(f"[INFO] Ejecuciones registradas: {len(ETL_EXECUTIONS)}")


if __name__ == '__main__':
    main()

