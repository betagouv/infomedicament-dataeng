"""Document-level outcomes carried between daily pipeline runs."""

from dataclasses import asdict, dataclass, field
from datetime import datetime


@dataclass
class DocumentFailure:
    cis: str
    document_type: str
    url: str | None
    stage: str
    error: str


@dataclass
class SemanticImportResult:
    watermark: datetime | None
    imported: dict[str, int] = field(default_factory=lambda: {"rcp": 0, "notice": 0})
    failed_documents: list[DocumentFailure] = field(default_factory=list)

    def report(self) -> dict:
        return {
            "imported": self.imported,
            "failed_count": len(self.failed_documents),
            "failed_documents": [asdict(document) for document in self.failed_documents],
        }
