-- ============================================================================
-- HEALTHCARE STAR SCHEMA - Optimizado para PowerBI
-- ============================================================================
-- 1 Tabla de Hechos + 7 Dimensiones
-- ============================================================================

-- Drop tablas existentes (para idempotencia)
DROP TABLE IF EXISTS fact_admisiones CASCADE;
DROP TABLE IF EXISTS dim_pacientes CASCADE;
DROP TABLE IF EXISTS dim_doctores CASCADE;
DROP TABLE IF EXISTS dim_hospitales CASCADE;
DROP TABLE IF EXISTS dim_seguros CASCADE;
DROP TABLE IF EXISTS dim_condiciones CASCADE;
DROP TABLE IF EXISTS dim_medicamentos CASCADE;
DROP TABLE IF EXISTS dim_pruebas CASCADE;

-- ============================================================================
-- DIMENSIONES
-- ============================================================================

-- Dimensión: Pacientes
CREATE TABLE dim_pacientes (
    patient_id SERIAL PRIMARY KEY,
    patient_name VARCHAR(255) NOT NULL UNIQUE,
    age INT,
    gender VARCHAR(10),
    blood_type VARCHAR(5)
);
CREATE INDEX idx_dim_pacientes_name ON dim_pacientes(patient_name);

-- Dimensión: Doctores
CREATE TABLE dim_doctores (
    doctor_id SERIAL PRIMARY KEY,
    doctor_name VARCHAR(255) NOT NULL UNIQUE
);
CREATE INDEX idx_dim_doctores_name ON dim_doctores(doctor_name);

-- Dimensión: Hospitales
CREATE TABLE dim_hospitales (
    hospital_id SERIAL PRIMARY KEY,
    hospital_name VARCHAR(255) NOT NULL UNIQUE
);
CREATE INDEX idx_dim_hospitales_name ON dim_hospitales(hospital_name);

-- Dimensión: Seguros
CREATE TABLE dim_seguros (
    insurance_id SERIAL PRIMARY KEY,
    provider_name VARCHAR(255) NOT NULL UNIQUE
);
CREATE INDEX idx_dim_seguros_provider ON dim_seguros(provider_name);

-- Dimensión: Condiciones Médicas
CREATE TABLE dim_condiciones (
    condition_id SERIAL PRIMARY KEY,
    condition_name VARCHAR(255) NOT NULL UNIQUE
);
CREATE INDEX idx_dim_condiciones_name ON dim_condiciones(condition_name);

-- Dimensión: Medicamentos
CREATE TABLE dim_medicamentos (
    medication_id SERIAL PRIMARY KEY,
    medication_name VARCHAR(255) NOT NULL UNIQUE
);
CREATE INDEX idx_dim_medicamentos_name ON dim_medicamentos(medication_name);

-- Dimensión: Pruebas
CREATE TABLE dim_pruebas (
    test_id SERIAL PRIMARY KEY,
    test_result VARCHAR(50) NOT NULL UNIQUE
);
CREATE INDEX idx_dim_pruebas_result ON dim_pruebas(test_result);

-- ============================================================================
-- TABLA DE HECHOS
-- ============================================================================

CREATE TABLE fact_admisiones (
    admission_id BIGSERIAL PRIMARY KEY,
    
    -- Foreign Keys a Dimensiones
    patient_id INT NOT NULL REFERENCES dim_pacientes(patient_id),
    doctor_id INT NOT NULL REFERENCES dim_doctores(doctor_id),
    hospital_id INT NOT NULL REFERENCES dim_hospitales(hospital_id),
    insurance_id INT NOT NULL REFERENCES dim_seguros(insurance_id),
    condition_id INT NOT NULL REFERENCES dim_condiciones(condition_id),
    medication_id INT NOT NULL REFERENCES dim_medicamentos(medication_id),
    test_id INT NOT NULL REFERENCES dim_pruebas(test_id),
    
    -- Medidas (Hechos)
    admission_date DATE NOT NULL,
    discharge_date DATE NOT NULL,
    age_at_admission INT,
    billing_amount NUMERIC(12, 2),
    room_number VARCHAR(10),
    admission_type VARCHAR(50),
    hospitalization_days INT,
    
    -- Metadata (histórico)
    data_load_date DATE DEFAULT CURRENT_DATE
);

-- Índices para performance en queries PowerBI
CREATE INDEX idx_fact_patient_id ON fact_admisiones(patient_id);
CREATE INDEX idx_fact_doctor_id ON fact_admisiones(doctor_id);
CREATE INDEX idx_fact_hospital_id ON fact_admisiones(hospital_id);
CREATE INDEX idx_fact_admission_date ON fact_admisiones(admission_date);
CREATE INDEX idx_fact_load_date ON fact_admisiones(data_load_date);
