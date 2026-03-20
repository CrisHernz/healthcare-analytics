"""
model_factory.py - Transforma DataFrame limpio a Modelo Estrella
"""

import pandas as pd


def create_star_schema(df: pd.DataFrame) -> dict:
    """
    Transforma DataFrame limpio a modelo estrella (1 hecho + 7 dimensiones)
    
    Args:
        df: DataFrame limpio con columnas: 
            Name, Age, Gender, Blood Type, Medical Condition,
            Date of Admission, Discharge Date, Doctor, Hospital,
            Insurance Provider, Billing Amount, Room Number,
            Admission Type, Medication, Test Results, Hospitalization_Days
    
    Returns:
        Dict con 8 DataFrames: fact_admisiones + 7 dimensiones
    """
    
    # ===== DIMENSIONES (extraer únicos) =====
    
    # dim_pacientes
    dim_pacientes = df[['Name', 'Age', 'Gender', 'Blood Type']].drop_duplicates(
        subset=['Name'], keep='first'
    ).reset_index(drop=True)
    dim_pacientes.insert(0, 'patient_id', range(1, len(dim_pacientes) + 1))
    dim_pacientes.rename(columns={
        'Name': 'patient_name',
        'Age': 'age',
        'Gender': 'gender',
        'Blood Type': 'blood_type'
    }, inplace=True)
    
    # dim_doctores
    dim_doctores = df[['Doctor']].drop_duplicates().reset_index(drop=True)
    dim_doctores.insert(0, 'doctor_id', range(1, len(dim_doctores) + 1))
    dim_doctores.rename(columns={'Doctor': 'doctor_name'}, inplace=True)
    
    # dim_hospitales
    dim_hospitales = df[['Hospital']].drop_duplicates().reset_index(drop=True)
    dim_hospitales.insert(0, 'hospital_id', range(1, len(dim_hospitales) + 1))
    dim_hospitales.rename(columns={'Hospital': 'hospital_name'}, inplace=True)
    
    # dim_seguros
    dim_seguros = df[['Insurance Provider']].drop_duplicates().reset_index(drop=True)
    dim_seguros.insert(0, 'insurance_id', range(1, len(dim_seguros) + 1))
    dim_seguros.rename(columns={'Insurance Provider': 'provider_name'}, inplace=True)
    
    # dim_condiciones
    dim_condiciones = df[['Medical Condition']].drop_duplicates().reset_index(drop=True)
    dim_condiciones.insert(0, 'condition_id', range(1, len(dim_condiciones) + 1))
    dim_condiciones.rename(columns={'Medical Condition': 'condition_name'}, inplace=True)
    
    # dim_medicamentos
    dim_medicamentos = df[['Medication']].drop_duplicates().reset_index(drop=True)
    dim_medicamentos.insert(0, 'medication_id', range(1, len(dim_medicamentos) + 1))
    dim_medicamentos.rename(columns={'Medication': 'medication_name'}, inplace=True)
    
    # dim_pruebas
    dim_pruebas = df[['Test Results']].drop_duplicates().reset_index(drop=True)
    dim_pruebas.insert(0, 'test_id', range(1, len(dim_pruebas) + 1))
    dim_pruebas.rename(columns={'Test Results': 'test_result'}, inplace=True)
    
    # ===== TABLA DE HECHOS =====
    
    # Crear mapeos para búsqueda rápida
    pacientes_map = dict(zip(dim_pacientes['patient_name'], dim_pacientes['patient_id']))
    doctores_map = dict(zip(dim_doctores['doctor_name'], dim_doctores['doctor_id']))
    hospitales_map = dict(zip(dim_hospitales['hospital_name'], dim_hospitales['hospital_id']))
    seguros_map = dict(zip(dim_seguros['provider_name'], dim_seguros['insurance_id']))
    condiciones_map = dict(zip(dim_condiciones['condition_name'], dim_condiciones['condition_id']))
    medicamentos_map = dict(zip(dim_medicamentos['medication_name'], dim_medicamentos['medication_id']))
    pruebas_map = dict(zip(dim_pruebas['test_result'], dim_pruebas['test_id']))
    
    # Crear tabla de hechos
    fact = pd.DataFrame()
    fact['patient_id'] = df['Name'].map(pacientes_map)
    fact['doctor_id'] = df['Doctor'].map(doctores_map)
    fact['hospital_id'] = df['Hospital'].map(hospitales_map)
    fact['insurance_id'] = df['Insurance Provider'].map(seguros_map)
    fact['condition_id'] = df['Medical Condition'].map(condiciones_map)
    fact['medication_id'] = df['Medication'].map(medicamentos_map)
    fact['test_id'] = df['Test Results'].map(pruebas_map)
    fact['admission_date'] = df['Date of Admission']
    fact['discharge_date'] = df['Discharge Date']
    fact['age_at_admission'] = df['Age']
    fact['billing_amount'] = df['Billing Amount']
    fact['room_number'] = df['Room Number']
    fact['admission_type'] = df['Admission Type']
    fact['hospitalization_days'] = df['Hospitalization_Days']
    fact['data_load_date'] = pd.Timestamp.now().date()
    
    # Agregar sequential admission_id
    fact.insert(0, 'admission_id', range(1, len(fact) + 1))
    
    return {
        'fact_admisiones': fact,
        'dim_pacientes': dim_pacientes,
        'dim_doctores': dim_doctores,
        'dim_hospitales': dim_hospitales,
        'dim_seguros': dim_seguros,
        'dim_condiciones': dim_condiciones,
        'dim_medicamentos': dim_medicamentos,
        'dim_pruebas': dim_pruebas,
    }
