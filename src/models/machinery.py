from datetime import date
from typing import Optional
from enum import Enum
from sqlmodel import SQLModel, Field
from pydantic import BaseModel, ConfigDict

class MachineryStatus(str, Enum):
    ACTIVE = "active"
    IN_MAINTENANCE = "in_maintenance"
    OUT_OF_SERVICE = "out_of_service"

class MachineryBase(SQLModel):
    vin: str = Field(index=True, unique=True, max_length=50, description="Vehicle Identification Number")
    model_name: str = Field(description="Name or type of the machinery model")
    hours: float = Field(default=0.0, description="Total registered hours of operation")
    last_maintenance: Optional[date] = Field(default=None, description="Date of the last registered maintenance")
    status: MachineryStatus = Field(default=MachineryStatus.ACTIVE, description="Current operational status")

class Machinery(MachineryBase, table=True):
    """
    SQLModel representation for the database schema.
    """
    id: Optional[int] = Field(default=None, primary_key=True)

class MachineryExternalPayload(BaseModel):
    """
    Pydantic Model for incoming generic/dirty data from external providers.
    It accepts permissive types (like strings for hours/dates) so the service layer can clean them.
    """
    vin: str
    model_name: str
    hours: float | int | str = 0.0
    last_maintenance: str | None = None
    status: str | None = None

    model_config = ConfigDict(extra="ignore")
