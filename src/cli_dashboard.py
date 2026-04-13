import os
import sys
from tabulate import tabulate

# To allow running from the root of the project:
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import (
    init_db,
    get_all_machinery,
    get_active_rentals,
    add_machinery,
    update_machinery_status,
)

# Colors
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"

def check_db():
    if not os.path.exists("data/walkia_master.db"):
        print(f"{YELLOW}Advertencia: Base de datos no encontrada. Inicializando una nueva...{RESET}")
        try:
            init_db()
            print(f"{GREEN}Base de datos y tablas creadas exitosamente.{RESET}")
        except Exception as e:
            print(f"{RED}Error fatal al crear la base de datos: {e}{RESET}")
            sys.exit(1)

def ver_flota():
    try:
        machinery = get_all_machinery()
        if not machinery:
            print(f"{YELLOW}No hay maquinaria registrada en la flota.{RESET}")
            return
        
        table = [[m.id, m.vin, m.brand, m.model_name, m.category, m.engine_hours, m.status] for m in machinery]
        headers = ["ID", "VIN", "Marca", "Modelo", "Categoría", "Horas", "Estado"]
        print(f"\n{CYAN}--- Flota de Maquinaria ---{RESET}")
        print(tabulate(table, headers=headers, tablefmt="fancy_grid"))
    except Exception as e:
        print(f"{RED}Error al acceder a la flota: {e}{RESET}")

def ver_alquileres_activos():
    try:
        rentals = get_active_rentals()
        if not rentals:
            print(f"{YELLOW}No hay alquileres activos en este momento.{RESET}")
            return
            
        table = [[r.id, r.machinery.model_name, r.machinery.vin, r.company.name, r.rental_date.strftime("%Y-%m-%d")] for r in rentals]
        headers = ["Rental ID", "Máquina", "VIN", "Empresa Alquila", "Fecha Inicio"]
        print(f"\n{CYAN}--- Alquileres Activos ---{RESET}")
        print(tabulate(table, headers=headers, tablefmt="fancy_grid"))
    except Exception as e:
        print(f"{RED}Error al acceder a los alquileres: {e}{RESET}")

def registrar_entrada():
    print(f"\n{CYAN}--- Registrar Nueva Maquinaria ---{RESET}")
    vin = input("VIN: ").strip()
    if not vin:
        print(f"{RED}El VIN es obligatorio.{RESET}")
        return

    model_name = input("Modelo: ").strip()
    brand = input("Marca [JCB]: ").strip() or "JCB"
    category = input("Categoría (Excavadora/Telescópica/Cargadora/Compactación): ").strip()
    
    try:
        hours_input = input("Horas de motor [0.0]: ").strip()
        hours = float(hours_input) if hours_input else 0.0
    except ValueError:
        print(f"{RED}Horas inválidas. Se asignará 0.0{RESET}")
        hours = 0.0
        
    try:
        machine = add_machinery(vin=vin, model_name=model_name, brand=brand, category=category, engine_hours=hours)
        print(f"{GREEN}Máquina '{machine.model_name}' (VIN: {machine.vin}) registrada con éxito.{RESET}")
    except Exception as e:
        print(f"{RED}Error al registrar la máquina: Quizás el VIN ya existe. ({e}){RESET}")

def actualizar_estado():
    print(f"\n{CYAN}--- Actualizar Estado ---{RESET}")
    vin = input("Introduce el VIN de la máquina a actualizar: ").strip()
    if not vin:
        return

    print("Opciones de estado:")
    print("  1. disponible")
    print("  2. alquilado")
    print("  3. taller")
    opcion = input("Selecciona nuevo estado (1-3): ").strip()
    estado_map = {"1": "disponible", "2": "alquilado", "3": "taller"}
    
    nuevo_estado = estado_map.get(opcion)
    if not nuevo_estado:
        print(f"{RED}Opción no válida.{RESET}")
        return
        
    try:
        machine = update_machinery_status(vin, nuevo_estado)
        if machine:
            print(f"{GREEN}Estado de la máquina {vin} actualizado a '{nuevo_estado}' con éxito.{RESET}")
        else:
            print(f"{RED}No se encontró maquinaria con el VIN '{vin}'.{RESET}")
    except Exception as e:
        print(f"{RED}Error al actualizar estado: {e}{RESET}")

def main():
    check_db()
    
    while True:
        print(f"\n{CYAN}=================================={RESET}")
        print(f"{CYAN}       WALKIA MVP DASHBOARD       {RESET}")
        print(f"{CYAN}=================================={RESET}")
        print("1. Ver Flota")
        print("2. Ver Alquileres Activos")
        print("3. Registrar Entrada")
        print("4. Actualizar Estado")
        print("5. Salir")
        print(f"{CYAN}=================================={RESET}")
        
        opcion = input("Elige una opción: ").strip()
        
        if opcion == "1":
            ver_flota()
        elif opcion == "2":
            ver_alquileres_activos()
        elif opcion == "3":
            registrar_entrada()
        elif opcion == "4":
            actualizar_estado()
        elif opcion == "5":
            print(f"{GREEN}Saliendo del dashboard... ¡Hasta luego!{RESET}")
            break
        else:
            print(f"{RED}Opción inválida. Intenta nuevamente.{RESET}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{GREEN}Saliendo de urgencia... ¡Hasta luego!{RESET}")
