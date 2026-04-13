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
    """
    Service to sanitize and validate incoming dirty data dictionaries.
    """
    
    @staticmethod
    def _parse_date(date_str: str | None) -> date | None:
        if not date_str:
            return None
        date_str = date_str.strip()
        formats = [
            "%Y/%m/%d", "%d-%m-%Y", "%Y.%m.%d", "%d/%m/%Y", "%Y-%m-%d"
        ]
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
            if fval < 0 or fval > 100000:
                fval = 0.0
            return round(fval, 2)
        except ValueError:
            return 0.0

    @staticmethod
    def process_companies(raw_list: list) -> list[dict]:
        cleaned = []
        with Progress() as progress:
            task = progress.add_task("[yellow]Limpiando Empresas...", total=len(raw_list))
            for raw in raw_list:
                try:
                    item = IncomingCompany(**raw)
                except ValidationError as e:
                    console.print(Panel(
                        f"[yellow]Fallo validación:[/yellow] {e.errors()[0]['msg']}\n[white]{raw}", 
                        title="Dato interceptado - Empresa", style="red"
                    ))
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
        return cleaned

    @staticmethod
    def process_machinery(raw_list: list) -> list[dict]:
        cleaned = []
        with Progress() as progress:
            task = progress.add_task("[blue]Limpiando Maquinaria...", total=len(raw_list))
            for raw in raw_list:
                try:
                    item = IncomingMachinery(**raw)
                except ValidationError as e:
                    console.print(Panel(
                        f"[yellow]Dato crítico inválido:[/yellow]\n[white]{raw}\n[red]{e.errors()}", 
                        title="Dato interceptado - Maquinaria", style="red"
                    ))
                    progress.advance(task)
                    continue
                
                vin = DataCleaner._clean_string(item.vin)
                model_name = DataCleaner._clean_string(item.model_name)
                if not vin or not model_name:
                    console.print(Panel(
                        f"[yellow]Falta VIN o Modelo:[/yellow]\n[white]{raw}", 
                        title="Dato interceptado - Maquinaria", style="red"
                    ))
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
                    
                m = {
                    "vin": vin,
                    "model_name": model_name,
                    "brand": brand,
                    "category": cat,
                    "engine_hours": DataCleaner._clean_numeric(item.engine_hours),
                    "status": st,
                    "hourly_rate": DataCleaner._clean_numeric(item.hourly_rate)
                }
                cleaned.append(m)
                progress.advance(task)
        return cleaned

    @staticmethod
    def process_rentals(raw_list: list) -> list[dict]:
        cleaned = []
        with Progress() as progress:
            task = progress.add_task("[green]Limpiando Alquileres...", total=len(raw_list))
            for raw in raw_list:
                try:
                    item = IncomingRental(**raw)
                except ValidationError as e:
                    console.print(Panel(
                        f"[yellow]Estructura inválida:[/yellow] {e.errors()[0]['msg']}\n[white]{raw}", 
                        title="Dato interceptado - Alquiler", style="red"
                    ))
                    progress.advance(task)
                    continue
                    
                start_date = DataCleaner._parse_date(item.rental_date)
                if not start_date:
                    console.print(Panel(
                        f"[yellow]Fecha inicio inválida:[/yellow] {item.rental_date}\n[white]{raw}", 
                        title="Dato interceptado - Alquiler", style="red"
                    ))
                    progress.advance(task)
                    continue
                    
                ret_date = DataCleaner._parse_date(item.return_date) if item.return_date else None
                est_hours = int(DataCleaner._clean_numeric(item.estimated_hours)) if item.estimated_hours else None
                
                r = {
                    "vin": DataCleaner._clean_string(item.vin),
                    "cif": DataCleaner._clean_string(item.cif),
                    "rental_date": start_date,
                    "return_date": ret_date,
                    "estimated_hours": est_hours if est_hours and est_hours > 0 else None
                }
                cleaned.append(r)
                progress.advance(task)
        return cleaned
