import logging
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from src.database import (
    init_db, add_company, add_machinery, create_rental, 
    get_company_by_cif, get_machinery_by_vin, get_all_machinery, 
    get_all_companies, get_active_rentals
)
from src.services.cleaner import DataCleaner

logger = logging.getLogger("WalkiaAPI")

app = FastAPI(
    title="Walkia Fleet Integration API",
    description="Capa intermedia experta de ingesta y limpieza de datos (ETL) para equipos pesados JCB, clientes y sus contratos de alquiler.",
    version="1.1.0",
)

@app.on_event("startup")
def on_startup():
    init_db()
    logger.info("Database initialized safely for FastAPI workers.")

class SyncPayload(BaseModel):
    companies: List[Dict[str, Any]] = []
    machinery: List[Dict[str, Any]] = []
    rentals: List[Dict[str, Any]] = []

@app.post("/api/sync", tags=["Sincronización (ETL)"], summary="Ingestar y Limpiar Datos")
def sync_machinery_data(payload: SyncPayload):
    """
    Recibe un JSON sucio (listado crudo de maquinaria, empresas o alquileres),
    lo somete a las rutinas de DataCleaner para validaciones estrictas tipo ETL,
    e intenta asentar la información en la base de datos SQL relacional.
    """
    companies_data = DataCleaner.process_companies(payload.companies)
    machinery_data = DataCleaner.process_machinery(payload.machinery)
    rentals_data = DataCleaner.process_rentals(payload.rentals)
    
    response = {
        "empresas": { "recibidos": len(payload.companies), "insertados": 0, "fallidos_db": 0, "errores_validacion": companies_data["errors"] },
        "maquinaria": { "recibidos": len(payload.machinery), "insertados": 0, "fallidos_db": 0, "errores_validacion": machinery_data["errors"] },
        "alquileres": { "recibidos": len(payload.rentals), "insertados": 0, "fallidos_db": 0, "descartados_por_integridad": 0, "errores_validacion": rentals_data["errors"] }
    }

    for comp in companies_data["cleaned"]:
        try:
            add_company(**comp)
            response["empresas"]["insertados"] += 1
        except Exception:
            response["empresas"]["fallidos_db"] += 1

    for mach in machinery_data["cleaned"]:
        try:
            add_machinery(**mach)
            response["maquinaria"]["insertados"] += 1
        except Exception:
            response["maquinaria"]["fallidos_db"] += 1

    for rent in rentals_data["cleaned"]:
        comp_obj = get_company_by_cif(rent['cif'])
        mach_obj = get_machinery_by_vin(rent['vin'])
        
        if not comp_obj or not mach_obj:
            response["alquileres"]["descartados_por_integridad"] += 1
            msg = f"Integridad Fallida: Empresa (CIF: {rent['cif']}) o Máquina (VIN: {rent['vin']}) ausentes en DB local."
            response["alquileres"]["errores_validacion"].append({"raw": rent, "error": msg})
            continue
            
        try:
            create_rental(
                machinery_id=mach_obj.id,
                company_id=comp_obj.id,
                rental_date=rent['rental_date'],
                return_date=rent['return_date'],
                estimated_hours=rent['estimated_hours']
            )
            response["alquileres"]["insertados"] += 1
        except Exception:
            response["alquileres"]["fallidos_db"] += 1

    return {
        "status": "success",
        "message": "Flujo de sincronización de flota ejecutado",
        "resumen": response
    }

@app.get("/api/machinery", tags=["Flota de Maquinaria"], summary="Obtener el catálogo de Maquinaria")
def get_machinery(status: Optional[str] = Query(None, description="Filtra la maquinaria por estado, ej: 'disponible', 'taller', 'alquilado'")):
    """
    Devuelve la flota completa registrada en la base de datos local. 
    Acepta un query parameter (status) para facilitar el filtrado.
    """
    machinery = get_all_machinery()
    if status:
        stat_lower = status.lower()
        machinery = [m for m in machinery if m.status.lower() == stat_lower]
    return machinery

@app.get("/api/machinery/{vin}", tags=["Flota de Maquinaria"], summary="Detalles de Máquina Específica")
def get_machinery_details(vin: str):
    """
    Recupera el perfil detallado de una máquina utilizando únicamente su VIN único.
    """
    machine = get_machinery_by_vin(vin.upper())
    if not machine:
        raise HTTPException(status_code=404, detail="Maquinaria no encontrada para el VIN especificado.")
    return machine

@app.get("/api/companies", tags=["Empresas Clientes"], summary="Directorio de Clientes")
def get_companies():
    """
    Emite el listado de todos los clientes corporativos o empresas en la plataforma.
    """
    return get_all_companies()

@app.get("/api/rentals/active", tags=["Operaciones"], summary="Obtener Alquileres Pendientes")
def get_active_rentals_endpoint():
    """
    Cruza y devuelve los alquileres que todavía no han reportado una fecha final o 'return_date'.
    La respuesta embebe a la entidad 'Company' y 'Machinery' gracias al ORM.
    """
    return get_active_rentals()
