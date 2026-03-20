"""
db_connector.py - Conexión PostgreSQL y carga de datos
"""

import pandas as pd
from sqlalchemy import create_engine, text


class PostgresLoader:
    """Carga DataFrames a PostgreSQL con truncate & carga (idempotente)"""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """
        Inicializa conexión a PostgreSQL
        """
        # Usar URL con parámetro de encoding latin1 para compatibilidad con CP1252
        connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?client_encoding=latin1"
        self.engine = create_engine(connection_string)
    
    def test_connection(self) -> bool:
        """Prueba la conexión a la BD"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            print(f" Error: {e}")
            return False
    
    def create_schema(self, schema_file: str) -> bool:
        """Ejecuta el script de schema.sql para crear tablas"""
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                sql_commands = f.read()
            
            with self.engine.connect() as conn:
                for command in sql_commands.split(';'):
                    command = command.strip()
                    if command:
                        conn.execute(text(command))
                conn.commit()
            
            return True
        except Exception as e:
            print(f" Error: {e}")
            return False
    
    def truncate_and_load(self, dataframes: dict, chunksize: int = 10000) -> bool:
        """
        Truncate & Load: limpia tablas y carga nuevos datos (idempotente)
        
        Args:
            dataframes: Dict {tabla: DataFrame}
            chunksize: Tamaño de chunks para inserción
            
        Returns:
            True si exitoso
        """
        try:
            # Orden de tablas (dimensiones primero, hechos último)
            table_order = [
                'dim_pacientes', 'dim_doctores', 'dim_hospitales',
                'dim_seguros', 'dim_condiciones', 'dim_medicamentos',
                'dim_pruebas', 'fact_admisiones'
            ]
            
            with self.engine.connect() as conn:
                # Deshabilitar FK temporalmente
                conn.execute(text("SET session_replication_role = 'replica'"))
                
                # Truncate (en orden inverso)
                for table in reversed(table_order):
                    if table in dataframes:
                        conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                
                # Habilitar FK
                conn.execute(text("SET session_replication_role = 'origin'"))
                conn.commit()
            
            # Insertar (en orden correcto)
            for table in table_order:
                if table in dataframes:
                    df = dataframes[table]
                    # Usar método simple de inserción (sin 'multi')
                    df.to_sql(table, self.engine, if_exists='append', index=False, 
                            chunksize=chunksize)
                    print(f" {table}: {len(df)} registros")
            
            return True
        
        except Exception as e:
            print(f" Error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_record_count(self, table_name: str) -> int:
        """Obtiene cantidad de registros"""
        try:
            result = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", self.engine)
            return result['cnt'][0]
        except:
            return 0
    
    def close(self):
        """Cierra la conexión"""
        self.engine.dispose()
