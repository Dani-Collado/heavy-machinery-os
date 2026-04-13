import os
from contextlib import contextmanager
from typing import List, Optional
from datetime import datetime
from sqlmodel import SQLModel, create_engine, Session, select

from .models.domain import Company, Machinery, Rental

DATABASE_URL = "sqlite:///data/walkia_master.db"
# ensure data folder exists
os.makedirs("data", exist_ok=True)

engine = create_engine(DATABASE_URL, echo=False)

def init_db():
    SQLModel.metadata.create_all(engine)

@contextmanager
def get_session():
    # expire_on_commit=False prevents detached instance errors when accessing properties outside scope
    session = Session(engine, expire_on_commit=False)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

# --- CRUD Functions ---

def add_machinery(vin: str, model_name: str, brand: str = "JCB", category: Optional[str] = None, 
                  engine_hours: float = 0.0, status: str = "disponible", hourly_rate: float = 0.0) -> Machinery:
    with get_session() as session:
        machine = session.exec(select(Machinery).where(Machinery.vin == vin)).first()
        if not machine:
            machine = Machinery(
                vin=vin, model_name=model_name, brand=brand, category=category,
                engine_hours=engine_hours, status=status, hourly_rate=hourly_rate
            )
            session.add(machine)
            session.commit()
            session.refresh(machine)
        return machine

def add_company(cif: str, name: str, industry: Optional[str] = None, location: Optional[str] = None) -> Company:
    with get_session() as session:
        company = session.exec(select(Company).where(Company.cif == cif)).first()
        if not company:
            company = Company(cif=cif, name=name, industry=industry, location=location)
            session.add(company)
            session.commit()
            session.refresh(company)
        return company

def create_rental(machinery_id: int, company_id: int, rental_date: datetime = None, return_date: datetime = None, estimated_hours: Optional[int] = None) -> Rental:
    with get_session() as session:
        rental = Rental(
            machinery_id=machinery_id,
            company_id=company_id,
            rental_date=rental_date or datetime.utcnow(),
            return_date=return_date,
            estimated_hours=estimated_hours
        )
        session.add(rental)
        session.commit()
        session.refresh(rental)
        return rental

def get_all_machinery() -> List[Machinery]:
    with get_session() as session:
        statement = select(Machinery)
        return list(session.exec(statement).all())

def get_active_rentals() -> List[Rental]:
    with get_session() as session:
        statement = select(Rental).where(Rental.return_date == None)
        return list(session.exec(statement).all())

def update_machinery_status(vin: str, new_status: str) -> Optional[Machinery]:
    with get_session() as session:
        machine = session.exec(select(Machinery).where(Machinery.vin == vin)).first()
        if machine:
            machine.status = new_status
            session.add(machine)
            return machine
        return None

def delete_machinery(vin: str) -> bool:
    with get_session() as session:
        machine = session.exec(select(Machinery).where(Machinery.vin == vin)).first()
        if machine:
            session.delete(machine)
            return True
        return False

def get_company_by_cif(cif: str) -> Optional[Company]:
    with get_session() as session:
        return session.exec(select(Company).where(Company.cif == cif)).first()

def get_machinery_by_vin(vin: str) -> Optional[Machinery]:
    with get_session() as session:
        return session.exec(select(Machinery).where(Machinery.vin == vin)).first()
