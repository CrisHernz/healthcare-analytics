# 🏥 Healthcare Analytics - Sistema ETL + Telegram Bot

**Sistema completo de ingesta, procesamiento y análisis de datos de salud con notificaciones en tiempo real**

---

## 📋 Contenido

- [Descripción General](#descripción-general)
- [Requisitos Previos](#requisitos-previos)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Tecnologías Utilizadas](#tecnologías-utilizadas)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Ejecución](#ejecución)
- [Comandos Disponibles](#comandos-disponibles)
- [Bot de Telegram](#bot-de-telegram)

---

## 🎯 Descripción General

Sistema **end-to-end** que automatiza el procesamiento de datos de admisiones hospitalarias:

1. **Limpieza automática** de datos mediante Jupyter Notebook
2. **Transformación ETL** con modelo estrella
3. **Carga a PostgreSQL** de 54,966 registros validados
4. **Notificaciones en tiempo real** vía Telegram Bot
5. **Consultas interactivas** desde el Bot

**Objetivo:** Procesar datos de salud, limpiarlos, cargarlos en BD y permitir análisis interactivo mediante Telegram.

---

## ⚙️ Requisitos Previos

### Software

- **Python 3.14+** (con pip)
- **PostgreSQL 18+** (codificación LATIN1)
- **Jupyter Notebook** (para visualizar datos)

### Intérprete de comandos

- Windows: CMD o PowerShell
- Linux/Mac: Terminal/Bash

### Conexión a Internet

- Requerida para Bot de Telegram
- Requerida para acceso a PostgreSQL (si está en servidor remoto)

---

## 📂 Estructura del Proyecto

```
Solucion/
│
├── 📄 README.md                   ← Este archivo
├── 📄 run.py                      ← Archivo el cual ejecuta el sistema
├── 📊 solucion.ipynb              ← Notebook de limpieza (Jupyter)
├── 📊 healthcare_dataset.csv      ← Datos originales
├── 📊 healthcare_dataset_cleaned.csv ← Datos limpios (generado)
│
├── ⚙️ scripts/
│   ├── main.py                    ← Orquestador ETL (Fases 1-8)
│   ├── data_cleaner.py            ← Ejecutor del notebook
│   ├── telegram_bot.py            ← Bot Telegram (4 comandos)
│   ├── model_factory.py           ← Generador Star Schema
│   ├── db_connector.py            ← Cargador PostgreSQL
│   └── __init__.py
│
├── 🔧 config/
│   ├── .env                       ← Credenciales (ajustarlas a su ambiente)
│   └── schema.sql                 ← Esquema PostgreSQL
│
├── 📦 requirements.txt            ← Dependencias Python
│
└── 🔐 env/                        ← Entorno virtual (crear con venv)
```

### Archivos Clave

| Archivo            | Propósito                             |
| ------------------ | ------------------------------------- |
| `run.py`           | Punto de entrada único - ejecuta todo |
| `scripts/main.py`  | Orquestador con 8 fases               |
| `solucion.ipynb`   | Notebook Jupyter (limpieza de datos)  |
| `config/.env`      | Variables de entorno (credenciales)   |
| `requirements.txt` | Lista de dependencias Python          |

---

## 🛠️ Tecnologías Utilizadas

### Backend

- **Python 3.14.3** - Lenguaje principal
- **Pandas 3.0.1** - Manipulación de datos
- **SQLAlchemy 2.0+** - ORM para BD
- **psycopg2-binary 2.9+** - Conector PostgreSQL

### Base de Datos

- **PostgreSQL 18** - OLAP relacional
- **Modelo Estrella (Star Schema)** - 1 Fact + 7 Dimensions
- **Codificación LATIN1** - Compatible con datos

### Bot & Notificaciones

- **pyTelegramBotAPI 4.32.0** - Integración Telegram
- **Threading** - Ejecución paralela ETL + Bot
- **Multi-usuario** - Soporta múltiples usuarios/chats

### Herramientas de Desarrollo

- **Jupyter Notebook** - EDA (Análisis Exploratorio)
- **nbconvert 7.0+** - Ejecución automática de notebooks
- **python-dotenv 1.0+** - Gestión de secretos

---

## 💾 Instalación

### Paso 1: Clonar/Descargar el Proyecto

```bash
# Si usas Git
git clone https://github.com/CrisHernz/healthcare-analytics.git
cd Solucion

# Si descargas .zip, extrae y entra en la carpeta
cd Solucion
```

### Paso 2: Crear Entorno Virtual

**Windows:**

```bash
python -m venv env
env\Scripts\activate.bat
```

**Linux/Mac:**

```bash
python3 -m venv env
source env/bin/activate
```

### Paso 3: Instalar Dependencias

```bash
pip install -r requirements.txt
```

**Dependencias clave:**

- pandas>=2.0
- sqlalchemy>=2.0
- psycopg2-binary>=2.9
- python-dotenv>=1.0
- pyTelegramBotAPI>=4.0
- nbconvert>=7.0
- jupyter>=1.0

### Paso 4: Crear Base de Datos PostgreSQL

```bash
# Conectarse a PostgreSQL (requiere credenciales)
psql -U postgres

# En la terminal de PostgreSQL:
CREATE DATABASE healthcare_db
  WITH ENCODING = 'LATIN1'
  LC_COLLATE = 'C'
  LC_CTYPE = 'C';

# Verificar
\list

# Salir
\q
```

---

## 🔐 Configuración

### Crear archivo `config/.env`

Crea un archivo llamado `.env` en la carpeta `config/`:

```env
# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=healthcare_db
DB_USER=postgres
DB_PASSWORD=tu_contraseña_postgres

# Telegram Bot
TELEGRAM_TOKEN=8740557897:AAGfmR6aD1e3qGdtEGgyE-pU6-nNRj01ccY
TELEGRAM_CHAT_ID=(vacio)

# Schema
SCHEMA_FILE=config/schema.sql
```

---

## 🚀 Ejecución

### Opción 1: Ejecutar Todo con run.py (RECOMENDADO)

```bash
python run.py
```

**¿Qué sucede?**

1. Bot se levanta en background
2. Espera a que abras Telegram y hagas `/start`
3. Ejecuta notebook (limpieza)
4. Carga datos en PostgreSQL
5. Notifica a Telegram
6. Bot continúa activo para consultas

### Duración Esperada

| Fase                 | Tiempo              |
| -------------------- | ------------------- |
| Fase 0: Bot init     | 1 segundo           |
| Fase 1: Notebook     | 2-3 segundos        |
| Fase 2: CSV load     | 1-2 segundos        |
| Fase 3: Star Schema  | 1 segundo           |
| Fase 4-6: PostgreSQL | 6-8 segundos        |
| Fase 7: Notificación | 1 segundo           |
| **TOTAL**            | **~12-15 segundos** |

---

## 💬 Comandos Disponibles

### Comandos del Bot de Telegram

| Comando                    | Función                     | Ejemplo                     |
| -------------------------- | --------------------------- | --------------------------- |
| `/start`                   | Abre el menú principal      | Muestra 4 botones           |
| `/status`                  | Estado actual del sistema   | Última ejecución, registros |
| `/help`                    | Información de comandos     | Lista de ayuda              |
| `Analizar Costos` (botón)  | Top 5 condiciones por costo | $50M, $48M, etc.            |
| `Top Medicamentos` (botón) | Top 5 medicamentos usados   | Aspirin, Ibuprofen, etc.    |
| `Análisis Riesgos` (botón) | Pacientes con LOS ≥ 7 días  | % pacientes, costo promedio |
| `Top Aseguradoras` (botón) | Top 3 planes de seguros     | Aetna, Cigna, Humana        |

### Flujo de Uso en Telegram

```
1. Abre Telegram
2. Busca: @notification_invers_test_bot
3. Click "Start" o escribe /start
4. Verás el menú con 4 opciones
5. Presiona un botón para ver análisis
```

---

## 🤖 Bot de Telegram

### Información del Bot

- **Nombre:** Healthcare Analytics Bot
- **Usuario:** `@notification_invers_test_bot`
- **URL Directa:** http://t.me/notification_invers_test_bot
- **Tipo:** Bot multi-usuario
- **Disponibilidad:** 24/7 (mientras `python run.py` esté activo)

### Cómo Acceder

#### Opción 1: Desde la URL

```
http://t.me/notification_invers_test_bot
```

Haz click y se abrirá Telegram automáticamente.

#### Opción 2: Desde Telegram

1. Abre Telegram
2. Busca: `@notification_invers_test_bot`
3. Haz click en el resultado
4. Presiona "Start"

#### Opción 3: Comando directo

```
/start
```

### Funcionalidades del Bot

✅ **Multi-usuario:** Cada usuario en su propio chat
✅ **Notificaciones automáticas:** Cuando ETL completa
✅ **Análisis interactivos:** 4 tipos de análisis
✅ **Estado en tiempo real:** `/status` muestra última ejecución
✅ **Sin base de datos externa:** Consulta PostgreSQL directamente
✅ **Manejo de errores:** Respuestas amigables ante fallos

---

### Modelo de Datos (Star Schema)

**Fact Table:**

```sql
fact_fact_healthcare (54,966 rows)
├─ patient_id (FK)
├─ admission_date_id (FK)
├─ condition_id (FK)
├─ medication_id (FK)
├─ provider_id (FK)
├─ insurance_id (FK)
├─ admission_type_id (FK)
├─ discharge_id (FK)
├─ cost (NUMERIC)
├─ length_of_stay (INTEGER)
└─ patient_age (INTEGER)
```

**Dimension Tables:**

- `dim_medical_condition` (56 únicas)
- `dim_medication` (85 únicas)
- `dim_provider` (50 proveedores)
- `dim_insurance` (10 planes)
- `dim_admission_type` (5 tipos)
- `dim_discharge_disposition` (8 disposiciones)
- `dim_date` (temporal)

## 📈 Monitoreo & Logs

### Ver Logs en Terminal

Mientras `python run.py` esté activo, verás en tiempo real:

```
[OK] Chat abierto - User 123456, Chat ID: 987654
Fase 1: Ejecutando notebook de limpieza...
[OK] PIPELINE COMPLETADO
[OK] Notificacion enviada a Telegram
Fase 8: Bot de Telegram ACTIVO - Esperando consultas...
```

### Detener el Sistema

```bash
# Presionar CTRL+C
# El sistema se detendrá gracefully:
[OK] Sistema detenido por usuario
```

---

## 🎓 Flujo Típico del Usuario

```
1. Ejecutar:        python run.py
2. Ver:             "⏳ Esperando que abras el chat en Telegram"
3. Acción:          Abre http://t.me/notification_invers_test_bot
4. Presionar:       /start o botón "Start"
5. Esperar:         ETL se ejecuta (12-15 segundos)
6. Recibir:         "NOTIFICACION: ETL COMPLETADO" en Telegram
7. Consultar:       Presiona botones para análisis
                    (Costos, Medicamentos, Riesgos, Aseguradoras)
8. Detener:         CTRL+C en terminal
```

---

## 📄 Licencia

Este proyecto es de código abierto. Úsalo libremente con fines educativos y académicos.
Con el respectivo apoyo a @CrisHernz

---

**Última actualización:** Marzo 2026

**Versión:** 1.0 (Bot-First Architecture)

**Autor:** Cristian Hernandez

---
