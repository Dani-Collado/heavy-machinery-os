-- 1. Tabla de Empresas Clientes
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cif TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    industry TEXT,
    location TEXT
);

-- 2. Tabla de Maquinaria (Catálogo)
CREATE TABLE IF NOT EXISTS machinery (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vin TEXT UNIQUE NOT NULL,
    brand TEXT DEFAULT 'JCB',
    model_name TEXT NOT NULL,
    category TEXT CHECK(category IN ('Excavadora', 'Telescópica', 'Cargadora', 'Compactación')),
    engine_hours REAL DEFAULT 0.0,
    status TEXT CHECK(status IN ('disponible', 'alquilado', 'taller')) DEFAULT 'disponible',
    hourly_rate REAL DEFAULT 0.0
);

-- 3. Tabla de Alquileres (Relación muchos a muchos)
CREATE TABLE IF NOT EXISTS rentals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    machinery_id INTEGER NOT NULL,
    company_id INTEGER NOT NULL,
    rental_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    return_date DATETIME,
    estimated_hours INTEGER,
    FOREIGN KEY (machinery_id) REFERENCES machinery(id),
    FOREIGN KEY (company_id) REFERENCES companies(id)
);