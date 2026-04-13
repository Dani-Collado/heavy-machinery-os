import re
from datetime import date, datetime
from pydantic import BaseModel, ValidationError
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress

console = Console()

# --- Pydantic Models for Validation ---

class IncomingCompany(BaseModel):
    cif: str
    name: str
    location: str | None = None
    industry: str | None = None

class IncomingMachinery(BaseModel):
    vin: str
    model_name: str
    brand: str | None = None
    category: str | None = None
    engine_hours: float | int | str | None = 0.0
    status: str | None = "disponible"
    hourly_rate: float | int | str | None = 0.0

class IncomingRental(BaseModel):
    vin: str
    cif: str
    rental_date: str
    return_date: str | None = None
    estimated_hours: int | str | float | None = None


class DataCleaner:
    
    @staticmethod
    def _parse_date(date_str: str | None) -> date | None:
        if not date_str:
            return None
        date_str = date_str.strip()
        formats = ["%Y/%m/%d", "%d-%m-%Y", "%Y.%m.%d", "%d/%m/%Y", "%Y-%m-%d"]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _clean_string(s: str | None) -> str | None:
        if not s:
            return None
        return " ".join(s.split()).upper()

    @staticmethod
    def _clean_numeric(val: float | int | str | None) -> float:
        if val is None:
            return 0.0
        try:
            if isinstance(val, str):
                val = re.sub(r'[^\d,\.-]', '', val)
                val = val.replace(',', '.')
            fval = float(val)
            return round(fval, 2)
        except ValueError:
            return 0.0

    @staticmethod
    def process_companies(raw_list: list) -> dict:
        cleaned = []
        errors = []
        with Progress() as progress:
            task = progress.add_task("[yellow]Limpiando Empresas...", total=len(raw_list))
            for raw in raw_list:
                try:
                    item = IncomingCompany(**raw)
                except ValidationError as e:
                    msg = f"Fallo validación: {e.errors()[0]['msg']}"
                    errors.append({"raw": raw, "error": msg})
                    console.print(Panel(f"[yellow]{msg}[/yellow]\n[white]{raw}", title="Dato interceptado - Empresa", style="red"))
                    progress.advance(task)
                    continue
                
                cif = DataCleaner._clean_string(item.cif)
                name = " ".join(item.name.split()).title()
                
                c = {
                    "cif": cif,
                    "name": name,
                    "location": " ".join(item.location.split()).title() if item.location else None,
                    "industry": " ".join(item.industry.split()).title() if item.industry else None,
                }
                cleaned.append(c)
                progress.advance(task)
        return {"cleaned": cleaned, "errors": errors}

    @staticmethod
    def process_machinery(raw_list: list) -> dict:
        cleaned = []
        errors = []
        with Progress() as progress:
            task = progress.add_task("[blue]Limpiando Maquinaria...", total=len(raw_list))
            for raw in raw_list:
                try:
                    item = IncomingMachinery(**raw)
                except ValidationError as e:
                    msg = f"Dato crítico inválido: {e.errors()}"
                    errors.append({"raw": raw, "error": msg})
                    console.print(Panel(f"[yellow]{msg}[/yellow]\n[white]{raw}", title="Dato interceptado - Maquinaria", style="red"))
                    progress.advance(task)
                    continue
                
                vin = DataCleaner._clean_string(item.vin)
                model_name = DataCleaner._clean_string(item.model_name)
                if not vin or not model_name:
                    msg = "Falta VIN o Modelo funcional."
                    errors.append({"raw": raw, "error": msg})
                    console.print(Panel(f"[yellow]{msg}[/yellow]\n[white]{raw}", title="Dato interceptado - Maquinaria", style="red"))
                    progress.advance(task)
                    continue
                    
                cat = item.category
                if not cat:
                    if any(x in model_name for x in ["3CX", "4CX", "JS"]):
                        cat = "Excavadora"
                    elif any(x in model_name for x in ["531", "540"]):
                        cat = "Telescópica"
                    elif "409" in model_name:
                        cat = "Cargadora"
                    elif "VMT" in model_name:
                        cat = "Compactación"
                if cat:
                    cat = " ".join(cat.split()).title()
                    
                brand = " ".join(item.brand.split()).upper() if item.brand else "JCB"
                
                s = item.status.strip().lower() if item.status else ""
                if "taller" in s or "repair" in s or "roto" in s:
                    st = "taller"
                elif "alqu" in s:
                    st = "alquilado"
                else:
                    st = "disponible"
                    
                engine_hours = DataCleaner._clean_numeric(item.engine_hours)
                if engine_hours < 0 or engine_hours > 500000:
                    msg = f"Horas de motor irreales: {engine_hours}"
                    errors.append({"raw": raw, "error": msg})
                    console.print(Panel(f"[yellow]{msg}[/yellow]\n[white]{raw}", title="Dato interceptado - Maquinaria", style="red"))
                    progress.advance(task)
                    continue

                m = {
                    "vin": vin,
                    "model_name": model_name,
                    "brand": brand,
                    "category": cat,
                    "engine_hours": engine_hours,
                    "status": st,
                    "hourly_rate": DataCleaner._clean_numeric(item.hourly_rate)
                }
                cleaned.append(m)
                progress.advance(task)
        return {"cleaned": cleaned, "errors": errors}

    @staticmethod
    def process_rentals(raw_list: list) -> dict:
        cleaned = []
        errors = []
        with Progress() as progress:
            task = progress.add_task("[green]Limpiando Alquileres...", total=len(raw_list))
            for raw in raw_list:
                try:
                    item = IncomingRental(**raw)
                except ValidationError as e:
                    msg = f"Estructura inválida: {e.errors()[0]['msg']}"
                    errors.append({"raw": raw, "error": msg})
                    console.print(Panel(f"[yellow]{msg}[/yellow]\n[white]{raw}", title="Dato interceptado - Alquiler", style="red"))
                    progress.advance(task)
                    continue
                    
                start_date = DataCleaner._parse_date(item.rental_date)
                if not start_date:
                    msg = f"Fecha inicio inválida: {item.rental_date}"
                    errors.append({"raw": raw, "error": msg})
                    console.print(Panel(f"[yellow]{msg}[/yellow]\n[white]{raw}", title="Dato interceptado - Alquiler", style="red"))
                    progress.advance(task)
                    continue
                    
                ret_date = DataCleaner._parse_date(item.return_date) if item.return_date else None
                est_hours_val = DataCleaner._clean_numeric(item.estimated_hours) if item.estimated_hours else None
                if est_hours_val is not None and est_hours_val < 0:
                    msg = f"Horas estimadas negativas: {est_hours_val}"
                    errors.append({"raw": raw, "error": msg})
                    console.print(Panel(f"[yellow]{msg}[/yellow]\n[white]{raw}", title="Dato interceptado - Alquiler", style="red"))
                    progress.advance(task)
                    continue
                    
                est_hours_int = int(est_hours_val) if est_hours_val is not None else None
                
                r = {
                    "vin": DataCleaner._clean_string(item.vin),
                    "cif": DataCleaner._clean_string(item.cif),
                    "rental_date": start_date,
                    "return_date": ret_date,
                    "estimated_hours": est_hours_int if est_hours_int and est_hours_int > 0 else None
                }
                cleaned.append(r)
                progress.advance(task)
        return {"cleaned": cleaned, "errors": errors}
