import os
import sys
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt

# To allow running from the root of the project:
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import (
    init_db,
    get_all_machinery,
    get_active_rentals,
    get_all_companies,
    get_machinery_by_vin,
    update_machinery_status,
)

console = Console()

def check_db():
    if not os.path.exists("data/nexus_master.db"):
        console.print("[yellow]Advertencia: Base de datos no encontrada. Inicializando una nueva...[/yellow]")
        try:
            init_db()
            console.print("[green]Base de datos y tablas creadas exitosamente.[/green]")
        except Exception as e:
            console.print(f"[bold red]Error fatal al crear la base de datos: {e}[/bold red]")
            sys.exit(1)

def consultar_flota():
    try:
        machinery = get_all_machinery()
        if not machinery:
            console.print("[yellow]No hay maquinaria registrada en la flota.[/yellow]")
            return
            
        table = Table(title="Flota de Maquinaria", show_header=True, header_style="bold cyan")
        table.add_column("ID", justify="right")
        table.add_column("VIN", style="bold")
        table.add_column("Marca")
        table.add_column("Modelo")
        table.add_column("Categoría")
        table.add_column("Horas", justify="right")
        table.add_column("Estado")
        
        for m in machinery:
            estado = m.status.lower()
            estado_formatted = f"[bold red]{estado}[/bold red]" if estado == "taller" else f"[bold green]{estado}[/bold green]" if estado == "disponible" else f"[bold yellow]{estado}[/bold yellow]"
            
            table.add_row(
                str(m.id),
                str(m.vin),
                str(m.brand),
                str(m.model_name),
                str(m.category),
                str(m.engine_hours),
                estado_formatted
            )
            
        console.print()
        console.print(table)
    except Exception as e:
        console.print(f"[bold red]Error al acceder a la flota: {e}[/bold red]")

def buscar_maquina():
    console.print("\n[bold cyan]--- Buscar Máquina por VIN ---[/bold cyan]")
    vin = Prompt.ask("Introduce el VIN")
    vin = vin.strip().upper()
    if not vin:
        return
        
    try:
        machine = get_machinery_by_vin(vin)
        if not machine:
            console.print(f"[yellow]⚠ Advertencia: No se encontró maquinaria con el VIN '{vin}'. Compruebe que está bien escrito.[/yellow]")
            return
            
        estado = machine.status.lower()
        color = "red" if estado == "taller" else "green" if estado == "disponible" else "yellow"
        
        content = (
            f"[bold]Marca/Modelo:[/bold] {machine.brand} {machine.model_name}\n"
            f"[bold]Categoría:[/bold]    {machine.category}\n"
            f"[bold]Horas Motor:[/bold]  {machine.engine_hours} hrs\n"
            f"[bold]Tarifa/Hora:[/bold]  {machine.hourly_rate} EUR\n"
            f"[bold]Estado Actual:[/bold] [{color}]{machine.status.upper()}[/{color}]"
        )
        
        panel = Panel(content, title=f"🚜 Detalles Industriales: {machine.vin}", border_style="cyan", padding=(1, 2))
        console.print()
        console.print(panel)
    except Exception as e:
        console.print(f"[bold red]Error en la búsqueda: {e}[/bold red]")

def ver_empresas():
    try:
        companies = get_all_companies()
        if not companies:
            console.print("[yellow]No hay empresas registradas.[/yellow]")
            return
            
        table = Table(title="Directorio Corporativo", show_header=True, header_style="bold magenta")
        table.add_column("ID", justify="right")
        table.add_column("CIF", style="bold")
        table.add_column("Nombre Corporativo", style="cyan")
        table.add_column("Industria")
        table.add_column("Sede / Localidad")
        
        for c in companies:
            table.add_row(
                str(c.id),
                c.cif,
                c.name,
                c.industry or "N/A",
                c.location or "N/A"
            )
            
        console.print()
        console.print(table)
    except Exception as e:
        console.print(f"[bold red]Error al cargar empresas: {e}[/bold red]")

def ver_alquileres_activos():
    try:
        rentals = get_active_rentals()
        if not rentals:
            console.print("[yellow]No hay alquileres activos en este momento.[/yellow]")
            return
            
        table = Table(title="Alquileres en Curso (Active)", show_header=True, header_style="bold yellow")
        table.add_column("Contrato", justify="right")
        table.add_column("Máquina", style="bold cyan")
        table.add_column("VIN")
        table.add_column("Cliente Asignado", style="bold magenta")
        table.add_column("Inicio Alquiler")
        table.add_column("Estimadas", justify="right")
        
        for r in rentals:
            table.add_row(
                str(r.id),
                f"{r.machinery.brand} {r.machinery.model_name}",
                r.machinery.vin,
                r.company.name,
                r.rental_date.strftime("%Y-%m-%d") if r.rental_date else "N/A",
                f"{r.estimated_hours}h" if r.estimated_hours else "N/A"
            )
            
        console.print()
        console.print(table)
    except Exception as e:
        console.print(f"[bold red]Error al acceder a los alquileres: {e}[/bold red]")

def actualizar_estado():
    console.print("\n[bold cyan]--- Actualizar Estado de Máquina ---[/bold cyan]")
    vin = Prompt.ask("Introduce el VIN de la máquina")
    vin = vin.strip().upper()
    if not vin:
        return

    console.print("Estados disponibles: [green]1. disponible[/green] | [yellow]2. alquilado[/yellow] | [red]3. taller[/red]")
    opcion = Prompt.ask("Selecciona nuevo estado", choices=["1", "2", "3"])
    estado_map = {"1": "disponible", "2": "alquilado", "3": "taller"}
    
    nuevo_estado = estado_map.get(opcion)
        
    try:
        machine = update_machinery_status(vin, nuevo_estado)
        if machine:
            color = "red" if nuevo_estado == "taller" else "green" if nuevo_estado == "disponible" else "yellow"
            console.print(f"[bold green]✓ ¡Actualizado![/bold green] La máquina {vin} ha pasado al estado [{color}]'{nuevo_estado}'[/{color}]")
        else:
            console.print(f"[yellow]⚠ Advertencia: Operación abortada. No se encontró maquinaria con el VIN '{vin}'.[/yellow]")
    except Exception as e:
        console.print(f"[bold red]Error al actualizar estado: {e}[/bold red]")

def main():
    check_db()
    
    while True:
        console.print("\n[bold cyan]===================================[/bold cyan]")
        console.print("[bold cyan]       NEXUS MVP DASHBOARD         [/bold cyan]")
        console.print("[bold cyan]===================================[/bold cyan]")
        console.print("[1] Consultar Flota")
        console.print("[2] Buscar Máquina por VIN")
        console.print("[3] Ver Empresas Clientes")
        console.print("[4] Ver Alquileres Activos")
        console.print("[5] Cambiar Estado de Máquina")
        console.print("[6] Salir")
        console.print("[bold cyan]===================================[/bold cyan]")
        
        opcion = Prompt.ask("\nElige una opción", choices=["1", "2", "3", "4", "5", "6"], default="6")
        
        if opcion == "1":
            consultar_flota()
        elif opcion == "2":
            buscar_maquina()
        elif opcion == "3":
            ver_empresas()
        elif opcion == "4":
            ver_alquileres_activos()
        elif opcion == "5":
            actualizar_estado()
        elif opcion == "6":
            console.print("\n[bold green]Saliendo del dashboard... ¡Hasta luego![/bold green]\n")
            break

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[bold green]Saliendo de urgencia... ¡Hasta luego![/bold green]")
