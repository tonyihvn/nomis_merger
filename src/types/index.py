from dataclasses import dataclass
from typing import Any, List, Dict

@dataclass
class DatabaseRecord:
    id: int
    data: Dict[str, Any]

@dataclass
class MergeResult:
    merged_records: List[DatabaseRecord]
    removed_records: List[DatabaseRecord]
    duplicate_ids: List[int]