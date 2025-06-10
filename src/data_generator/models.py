"""Models for the CDR/EDR generator."""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class TelecomEvent:
    """Représente un événement de télécommunication (CDR/EDR)."""
    event_id: str
    timestamp: datetime
    event_type: str  # voice, sms, data
    user_id: str
    destination_id: Optional[str]
    duration: Optional[float]  # en secondes
    volume: Optional[int]      # en bytes pour data
    cell_id: str
    technology: str           # 2G, 3G, 4G, 5G
    status: str              # completed, failed, etc.
    error_code: Optional[str]
    
    def to_dict(self) -> dict:
        """Convertit l'événement en dictionnaire."""
        return {
            'event_id': self.event_id,
            'timestamp': self.timestamp.isoformat(),
            'event_type': self.event_type,
            'user_id': self.user_id,
            'destination_id': self.destination_id,
            'duration': self.duration,
            'volume': self.volume,
            'cell_id': self.cell_id,
            'technology': self.technology,
            'status': self.status,
            'error_code': self.error_code
        }
