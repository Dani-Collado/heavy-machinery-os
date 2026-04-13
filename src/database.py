import os
from contextlib import contextmanager
from typing import List, Optional
from sqlmodel import SQLModel, create_engine, Session, select

from .models.domain import Company, Machinery, Rental

DATABASE_URL = "sqlite:///data/walkia_master.db"
# ensure data folder exists
os.makedirs("data", exist_ok=True)

engine = create_engine(DATABASE_URL, echo=False)

def init_db():
    """Initializes the database by creating all tables if they don't exist."""
    SQLModel.metadata.create_all(engine)

@contextmanager
def get_session():
    """Context manager for safely handling database sessions."""
    session = Session(engine)
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
        machine = Machinery(
            vin=vin, model_name=model_name, brand=brand, category=category,
            engine_hours=engine_hours, status=status, hourly_rate=hourly_rate
        )
        session.add(machine)
        return machine

def add_company(cif: str, name: str, industry: Optional[str] = None, location: Optional[str] = None) -> Company:
    with get_session() as session:
        company = Company(cif=cif, name=name, industry=industry, location=location)
        session.add(company)
        return company

def create_rental(machinery_id: int, company_id: int, estimated_hours: Optional[int] = None) -> Rental:
    with get_session() as session:
        rental = Rental(
            machinery_id=machinery_id,
            company_id=company_id,
            estimated_hours=estimated_hours
        )
        session.add(rental)
        return rental

def get_all_machinery() -> List[Machinery]:
    with get_session() as session:
        statement = select(Machinery)
        return session.exec(statement).all()

def get_active_rentals() -> List[Rental]:
    with get_session() as session:
        # Assuming an active rental is one where return_date is None
        statement = select(Rental).where(Rental.return_date == None)
        return session.exec(statement).all()

def update_machinery_status(vin: str, new_status: str) -> Optional[Machinery]:
    with get_session() as session:
        statement = select(Machinery).where(Machinery.vin == vin)
        machine = session.exec(statement).first()
        if machine:
            machine.status = new_status
            session.add(machine)
            return machine
        return None

def delete_machinery(vin: str) -> bool:
    with get_session() as session:
        statement = select(Machinery).where(Machinery.vin == vin)
        machine = session.exec(statement).first()
        if machine:
            session.delete(machine)
            return True
        return False
