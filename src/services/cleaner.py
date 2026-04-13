import logging
from datetime import date, datetime
from pydantic import ValidationError

from src.models.machinery import MachineryExternalPayload, MachineryStatus, MachineryBase

logger = logging.getLogger(__name__)

class DataCleanerError(Exception):
    """Custom exception for data cleaning errors."""
    pass

class MachineryDataCleaner:
    @staticmethod
    def clean_machinery_data(raw_payload: dict) -> MachineryBase:
        """
        Takes a raw dictionary from an external provider, validates it minimally via Pydantic,
        and applies cleaning logic to return a properly formatted MachineryBase model ready for DB insertion.
        """
        try:
            raw_data = MachineryExternalPayload(**raw_payload)
        except ValidationError as e:
            logger.error(f"Pydantic schema validation failed: {e}")
            raise DataCleanerError(f"Invalid payload structure: {e}")

        clean_vin = MachineryDataCleaner._clean_vin(raw_data.vin)
        clean_model = MachineryDataCleaner._clean_string(raw_data.model_name)
        clean_hours = MachineryDataCleaner._clean_hours(raw_data.hours)
        clean_date = MachineryDataCleaner._parse_date(raw_data.last_maintenance) if raw_data.last_maintenance else None
        clean_status = MachineryDataCleaner._normalize_status(raw_data.status)

        return MachineryBase(
            vin=clean_vin,
            model_name=clean_model,
            hours=clean_hours,
            last_maintenance=clean_date,
            status=clean_status
        )

    @staticmethod
    def _clean_vin(vin: str) -> str:
        if not vin:
            raise DataCleanerError("VIN cannot be empty.")
        # Remove any whitespace and make uppercase
        return "".join(vin.split()).upper()

    @staticmethod
    def _clean_string(val: str) -> str:
        return val.strip() if val else ""

    @staticmethod
    def _clean_hours(hours: float | int | str) -> float:
        try:
            if isinstance(hours, str):
                # Handle possible comma instead of dot for decimals
                hours = hours.replace(",", ".")
            val = float(hours)
            if val < 0:
                raise ValueError("Hours cannot be negative.")
            return round(val, 2)
        except (ValueError, TypeError) as e:
            raise DataCleanerError(f"Invalid format for hours '{hours}': {e}")

    @staticmethod
    def _parse_date(date_str: str) -> date:
        date_str = date_str.strip()
        # Attempt multiple typical date formats
        formats = (
            "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y",
            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S"
        )
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.date()
            except ValueError:
                continue
        raise DataCleanerError(f"Could not parse valid date from: {date_str}")

    @staticmethod
    def _normalize_status(status_str: str | None) -> MachineryStatus:
        if not status_str:
            return MachineryStatus.ACTIVE
            
        normalized = status_str.strip().lower()
        
        if normalized in ("active", "activo", "ok", "working"):
            return MachineryStatus.ACTIVE
        elif normalized in ("maintenance", "mantenimiento", "in_maintenance", "repair", "broken"):
            return MachineryStatus.IN_MAINTENANCE
        elif normalized in ("out_of_service", "fuera_de_servicio", "retired", "inactive"):
            return MachineryStatus.OUT_OF_SERVICE
            
        logger.warning(f"Unrecognized status '{status_str}'. Defaulting to IN_MAINTENANCE to be safe.")
        return MachineryStatus.IN_MAINTENANCE
