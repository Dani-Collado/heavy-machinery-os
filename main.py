import json
import logging
from src.database import (
    init_db, add_company, add_machinery, create_rental, 
    get_company_by_cif, get_machinery_by_vin
)
from src.services.cleaner import DataCleaner

# Configure simple base logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("Orchestrator")

# ANSI Colors
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"

def main():
    logger.info(f"{CYAN}=== INICIALIZANDO WALKÍA MVP ORCHESTRATOR (ETL) ==={RESET}")
    
    # 1. Init DB
    logger.info(f"{CYAN}1. Preparando base de datos SQLite...{RESET}")
    try:
        init_db()
    except Exception as e:
        logger.error(f"{RED}Error crítico al inicializar la base de datos: {e}{RESET}")
        return

    # 2. Load and parse JSON
    logger.info(f"{CYAN}2. Cargando set de datos sucios desde data/raw_data.json...{RESET}")
    try:
        with open("data/raw_data.json", "r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except FileNotFoundError:
        logger.error(f"{RED}No se encontró 'data/raw_data.json'. Por favor, asegúrate de generar el dataset primero.{RESET}")
        return
        
    raw_companies = raw_data.get("companies", [])
    raw_machinery = raw_data.get("machinery", [])
    raw_rentals = raw_data.get("rentals", [])
    
    # 3. Clean and Persist Companies
    logger.info(f"{CYAN}\n--- PROCESANDO EMPRESAS ---{RESET}")
    cleaned_companies = DataCleaner.process_companies(raw_companies)
    inserted_companies = 0
    failed_companies = 0
    for comp in cleaned_companies:
        try:
            add_company(**comp)
            inserted_companies += 1
        except Exception as e:
            logger.error(f"{RED}[ERROR DB] Fallo al insertar empresa {comp['cif']}: {e}{RESET}")
            failed_companies += 1

    # 4. Clean and Persist Machinery
    logger.info(f"{CYAN}\n--- PROCESANDO MAQUINARIA ---{RESET}")
    cleaned_machinery = DataCleaner.process_machinery(raw_machinery)
    inserted_machinery = 0
    failed_machinery = 0
    for mach in cleaned_machinery:
        try:
            add_machinery(**mach)
            inserted_machinery += 1
        except Exception as e:
            logger.error(f"{RED}[ERROR DB] Fallo al insertar maquinaria {mach['vin']}: {e}{RESET}")
            failed_machinery += 1

    # 5. Clean and Persist Rentals
    logger.info(f"{CYAN}\n--- PROCESANDO ALQUILERES ---{RESET}")
    cleaned_rentals = DataCleaner.process_rentals(raw_rentals)
    inserted_rentals = 0
    failed_rentals = 0
    skipped_rentals = 0
    
    # Pre-caching to accelerate a bit
    for rent in cleaned_rentals:
        comp_obj = get_company_by_cif(rent['cif'])
        mach_obj = get_machinery_by_vin(rent['vin'])
        
        if not comp_obj or not mach_obj:
            logger.warning(f"{YELLOW}[SKIPPED] Alquiler omitido - Integridad: La Empresa (CIF: {rent['cif']}) o Máquina (VIN: {rent['vin']}) carecen de registros válidos.{RESET}")
            skipped_rentals += 1
            continue
            
        try:
            create_rental(
                machinery_id=mach_obj.id,
                company_id=comp_obj.id,
                rental_date=rent['rental_date'],
                return_date=rent['return_date'],
                estimated_hours=rent['estimated_hours']
            )
            inserted_rentals += 1
            logger.info(f"{GREEN}[SUCCESS] Contrato de Alquiler asentado: Máquina [{mach_obj.vin}] para [{comp_obj.name}]{RESET}")
        except Exception as e:
            logger.error(f"{RED}[ERROR DB] Fallo crítico al registrar contrato: {e}{RESET}")
            failed_rentals += 1

    # 6. Executive Summary
    total_failed = failed_companies + failed_machinery + failed_rentals
    total_skipped = (len(raw_companies) - len(cleaned_companies)) + \
                    (len(raw_machinery) - len(cleaned_machinery)) + \
                    (len(raw_rentals) - len(cleaned_rentals)) + skipped_rentals
                    
    logger.info(f"\n{CYAN}===================================================={RESET}")
    logger.info(f"{CYAN}           RESUMEN EJECUTIVO (ETL PROCESS)          {RESET}")
    logger.info(f"{CYAN}===================================================={RESET}")
    logger.info(f"{GREEN}✓ {inserted_machinery} Máquinas cargadas exitosamente.{RESET}")
    logger.info(f"{GREEN}✓ {inserted_companies} Empresas registradas exitosamente.{RESET}")
    logger.info(f"{GREEN}✓ {inserted_rentals} Alquileres creados exitosamente.{RESET}")
    logger.info(f"")
    logger.info(f"{YELLOW}⚠ {total_skipped} Registros saltados o descartados (Limpieza o Relación){RESET}")
    logger.info(f"{RED}✕ {total_failed} Registros fallidos por conflictos en base de datos{RESET}")
    logger.info(f"{CYAN}===================================================={RESET}\n")

if __name__ == "__main__":
    main()
