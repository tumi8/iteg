import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from src.utils import get_file_suffixes


@dataclass
class TLSScan:
    date: date
    location: Path

    def __init__(self, scan_date, scan_location):
        if isinstance(scan_date, str):
            self.date = date.fromisoformat(scan_date)
        else:
            self.date = scan_date
        self.location = Path(scan_location)

    def name(self) -> str:
        return os.path.basename(self.location)

    def certs(self) -> Path:
        return get_file_suffixes(self.location / 'certs.csv')

    def http(self) -> Path:
        return get_file_suffixes(self.location / 'http.csv')

    def hosts(self) -> Path:
        return get_file_suffixes(self.location / 'hosts.csv')


