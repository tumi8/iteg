import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass
class DNSScan:
    type: str
    date: date
    location: Path

    def __init__(self, scan_type, scan_date, scan_location):
        self.type = scan_type
        self.date = date.fromisoformat(scan_date)
        self.location = Path(scan_location)
        if not self.location.exists() or not self.location.is_file():
            raise Exception(f'File {self.location} is no DNS scan')

    def name(self):
        return os.path.basename(self.location)
