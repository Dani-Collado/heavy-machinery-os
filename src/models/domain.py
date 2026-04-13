from typing import Optional, List
from datetime import datetime
from sqlmodel import SQLModel, Field, Relationship

class Company(SQLModel, table=True):
    __tablename__ = "companies"
    id: Optional[int] = Field(default=None, primary_key=True)
    cif: str = Field(unique=True, index=True)
    name: str
    industry: Optional[str] = None
    location: Optional[str] = None

    rentals: List["Rental"] = Relationship(back_populates="company")

class Machinery(SQLModel, table=True):
    __tablename__ = "machinery"
    id: Optional[int] = Field(default=None, primary_key=True)
    vin: str = Field(unique=True, index=True)
    brand: str = Field(default="JCB")
    model_name: str
    category: Optional[str] = None
    engine_hours: float = Field(default=0.0)
    status: str = Field(default="disponible")
    hourly_rate: float = Field(default=0.0)

    rentals: List["Rental"] = Relationship(back_populates="machinery")

class Rental(SQLModel, table=True):
    __tablename__ = "rentals"
    id: Optional[int] = Field(default=None, primary_key=True)
    machinery_id: int = Field(foreign_key="machinery.id")
    company_id: int = Field(foreign_key="companies.id")
    rental_date: datetime = Field(default_factory=datetime.utcnow)
    return_date: Optional[datetime] = None
    estimated_hours: Optional[int] = None

    machinery: Machinery = Relationship(back_populates="rentals")
    company: Company = Relationship(back_populates="rentals")
