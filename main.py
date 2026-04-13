import json
from rich.console import Console
from rich.status import Status
from rich.progress import Progress
from rich.table import Table

from src.database import (
    init_db, add_company, add_machinery, create_rental, 
    get_company_by_cif, get_machinery_by_vin
)
from src.services.cleaner import DataCleaner

console = Console()

def main():
    console.rule("[cyan]WALKÍA MVP ORCHESTRATOR (ETL)")
    
    # 1 & 2. Init DB and Load JSON via Live Status
    with console.status("[bold green]🚜 Preparando Base de Datos y cargando JSON...") as status:
        try:
            init_db()
        except Exception as e:
            console.print(f"[bold red]Error crítico al inicializar la base de datos:[/bold red] {e}")
            return
            
        try:
            with open("data/raw_data.json", "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        except FileNotFoundError:
            console.print("[bold red]No se encontró 'data/raw_data.json'. Por favor, asegúrate de generar el dataset primero.[/bold red]")
            return
            
        raw_companies = raw_data.get("companies", [])
        raw_machinery = raw_data.get("machinery", [])
        raw_rentals = raw_data.get("rentals", [])
        
        status.update("[bold green]🚜 Validando y limpiando Empresas (Memoria)...")
        cleaned_companies = DataCleaner.process_companies(raw_companies)
        
        status.update("[bold green]🚜 Validando y limpiando Maquinaria (Memoria)...")
        cleaned_machinery = DataCleaner.process_machinery(raw_machinery)
        
        status.update("[bold green]🚜 Validando y limpiando Alquileres (Memoria)...")
        cleaned_rentals = DataCleaner.process_rentals(raw_rentals)

    # 3. Persist Companies
    console.print("\n[bold cyan]--- Persistiendo Empresas en SQLite ---")
    inserted_companies = 0
    failed_companies = 0
    with Progress() as progress:
        task = progress.add_task("[yellow]Insertando DB...", total=len(cleaned_companies))
        for comp in cleaned_companies:
            try:
                add_company(**comp)
                inserted_companies += 1
            except Exception as e:
                console.print(f"[red][ERROR DB] Fallo al insertar empresa {comp['cif']}: {e}[/red]")
                failed_companies += 1
            progress.advance(task)

    # 4. Persist Machinery
    console.print("\n[bold cyan]--- Persistiendo Maquinaria en SQLite ---")
    inserted_machinery = 0
    failed_machinery = 0
    with Progress() as progress:
        task = progress.add_task("[blue]Insertando DB...", total=len(cleaned_machinery))
        for mach in cleaned_machinery:
            try:
                add_machinery(**mach)
                inserted_machinery += 1
            except Exception as e:
                console.print(f"[red][ERROR DB] Fallo al insertar maquinaria {mach['vin']}: {e}[/red]")
                failed_machinery += 1
            progress.advance(task)

    # 5. Persist Rentals
    console.print("\n[bold cyan]--- Evaluando Relaciones y Persistiendo Alquileres ---")
    inserted_rentals = 0
    failed_rentals = 0
    skipped_rentals = 0
    
    with Progress() as progress:
        task = progress.add_task("[green]Insertando DB...", total=len(cleaned_rentals))
        for rent in cleaned_rentals:
            comp_obj = get_company_by_cif(rent['cif'])
            mach_obj = get_machinery_by_vin(rent['vin'])
            
            if not comp_obj or not mach_obj:
                console.print(f"[yellow]⚠ Integridad Fallida: Empresa (CIF: {rent['cif']}) o Máquina (VIN: {rent['vin']}) ausentes en DB.[/yellow]")
                skipped_rentals += 1
                progress.advance(task)
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
            except Exception as e:
                console.print(f"[red][ERROR DB] Fallo al registrar contrato: {e}[/red]")
                failed_rentals += 1
            progress.advance(task)

    # 6. Executive Summary via Rich Live Table
    
    table = Table(title="RESUMEN EJECUTIVO (ETL PROCESS)", show_header=True, header_style="bold magenta")
    table.add_column("Entidad", style="cyan", width=20)
    table.add_column("Total JSON", justify="right")
    table.add_column("Exitosos (DB)", justify="right", style="green")
    table.add_column("Rechazados / Error", justify="right", style="red")

    table.add_row(
        "🏭 Empresas", 
        str(len(raw_companies)), 
        str(inserted_companies), 
        str((len(raw_companies) - len(cleaned_companies)) + failed_companies)
    )
    table.add_row(
        "🚜 Maquinaria", 
        str(len(raw_machinery)), 
        str(inserted_machinery), 
        str((len(raw_machinery) - len(cleaned_machinery)) + failed_machinery)
    )
    table.add_row(
        "📝 Alquileres", 
        str(len(raw_rentals)), 
        str(inserted_rentals), 
        str((len(raw_rentals) - len(cleaned_rentals)) + failed_rentals + skipped_rentals)
    )
    
    console.print("")
    console.print(table)
    console.print("[bold green]¡Proceso ETL completado con éxito![/bold green]")

if __name__ == "__main__":
    main()
