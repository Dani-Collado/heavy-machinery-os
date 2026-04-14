import json
import random
from datetime import datetime, timedelta
import os

def random_date(start: datetime, end: datetime) -> datetime:
    return start + timedelta(days=random.randint(0, int((end - start).days)))

def format_dirty_date(dt: datetime) -> str:
    formats = [
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%Y.%m.%d",
        "%d/%m/%Y",
        "%Y-%m-%d"
    ]
    return dt.strftime(random.choice(formats))

def generate_dirty_data():
    companies = []
    machinery = []
    rentals = []

    # 1. Base data sets
    cifs = [f"B{str(i).zfill(8)}" for i in range(1, 151)] # 150 unique companies
    base_company_names = ["Construcciones", "Excavaciones", "Obras", "Alquileres", "Movimientos de Tierra", "Ingeniería"]
    suffixes = ["S.L.", "SL", "S.A.", "SA", "Hnos.", "y Asociados"]
    locations = ["Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao", "Málaga", "Zaragoza", "Murcia", "Valladolid", "Alicante"]
    industries = ["Construcción", "Minería", "Obra Civil", "Agricultura", "Logística"]

    # Generate 180 companies (30 duplicates with same CIF but slightly different name intentionally)
    for i in range(180):
        # The first 150 are unique CIFs. The last 30 will reuse a random previous CIF to simulate duplication/inconsistency
        if i < 150:
            cif = cifs[i]
        else:
            cif = random.choice(cifs[:50]) # ensure some overlap
            
        name_base = random.choice(base_company_names) + " " + random.choice(["García", "Martínez", "López", "Ibérea", "Norte", "Sur", "Levante", "Central"])
        suffix = random.choice(suffixes)
        name = f"{name_base} {suffix}"
        
        company = {
            "cif": cif,
            "name": name,
            "location": random.choice(locations)
        }
        
        # Missing fields simulation
        if random.random() > 0.15: # 15% will miss industry
            company["industry"] = random.choice(industries)
            
        companies.append(company)

    # 2. Machinery Data Sets
    brands = ["JCB", "jcb", "  JCB", "JcB", None]
    categories = ["Excavadora", "Telescópica", "Cargadora", "Compactación"]
    models = ["3CX", "4CX", "JS220", "531-70", "540-140", "19C-1", "VMT260", "409", "Teletruk 30D"]
    
    vins = [f"JCB{str(i).zfill(8)}X" for i in range(1, 151)] # 150 machines
    
    for vin in vins:
        model = random.choice(models)
        
        # Apply string inconsistency rules to model
        dirt_type = random.randint(1, 6)
        if dirt_type == 1:
            model = f"  {model.lower()} "
        elif dirt_type == 2:
            model = model.replace("-", "_")
        elif dirt_type == 3:
            model = model.lower()
        elif dirt_type == 4:
            model = f"{model}  "
            
        machine = {
            "vin": vin,
            "model_name": model,
        }
        
        if random.random() > 0.2: # 20% miss brand completely (testing defaults)
            brand_val = random.choice(brands)
            if brand_val is not None:
                machine["brand"] = brand_val
                
        if random.random() > 0.1: # 10% miss category
            machine["category"] = random.choice(categories)
            
        # Error simulation on numerical values
        hours_rand = random.random()
        if hours_rand < 0.05:
            machine["engine_hours"] = random.uniform(-100, -1) # Negative hours
        elif hours_rand < 0.1:
            machine["engine_hours"] = random.uniform(50000, 999999) # Outlier/Extreme
        elif hours_rand < 0.2:
            machine["engine_hours"] = f"{random.randint(100, 5000)},{random.randint(0, 99)}" # String with European comma notation
        elif hours_rand < 0.3:
            machine["engine_hours"] = f"{random.randint(500, 2000)} hrs" # String with suffix
        else:
            machine["engine_hours"] = random.uniform(0, 15000) # Normal float
            
        machine["status"] = random.choice(["disponible", "alquilado", "taller", "  TALLer ", "DISPONIBLE", "repair", "ROTO!!"])
        
        # Include hourly rates with dirty floats
        if random.random() > 0.5:
            machine["hourly_rate"] = random.choice([25.5, 30.0, "30,5", 45, "50.5 EUR", -10.0])

        machinery.append(machine)

    # 3. Rentals Setup
    start_dt = datetime(2022, 1, 1)
    end_dt = datetime(2024, 12, 31)
    
    for i in range(200): # 200 rental contracts
        m_vin = random.choice(vins)
        c_cif = random.choice(cifs)
        r_start = random_date(start_dt, end_dt)
        r_end = r_start + timedelta(days=random.randint(1, 180))
        
        rental = {
            "vin": m_vin,
            "cif": c_cif,
            "rental_date": format_dirty_date(r_start) # varied date string
        }
        
        if random.random() > 0.3: # Return date is only present in some completed contracts
            rental["return_date"] = format_dirty_date(r_end)
            
        hours_rand = random.random()
        if hours_rand > 0.1:
             # Estimated hours with some dirt
             if hours_rand > 0.9:
                 rental["estimated_hours"] = str(random.randint(10, 500))
             elif hours_rand > 0.85:
                 rental["estimated_hours"] = -5 # Invalid negative estimation
             else:
                 rental["estimated_hours"] = random.randint(10, 1000)
                 
        rentals.append(rental)

    out = {
        "companies": companies,
        "machinery": machinery,
        "rentals": rentals
    }
    
    os.makedirs("data", exist_ok=True)
    with open("data/raw_data.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    generate_dirty_data()
    print("El archivo 'data/raw_data.json' ha sido generado exitosamente con datos de prueba sucios.")
