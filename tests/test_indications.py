"""Tests for the PostgreSQL-only indications builder."""

from contextlib import nullcontext
from unittest.mock import MagicMock

from infomedicament_dataeng import indications
from infomedicament_dataeng.indications import _clinical_class_name, _plan_indications, build_indications


def test_clinical_class_name_matches_previous_formatting():
    assert _clinical_class_name("  ASTHME  ") == "Asthme"
    assert _clinical_class_name("douleur") == "douleur"
    assert _clinical_class_name("ÉPILEPSIE") == "ÉPILEPSIE"
    assert _clinical_class_name(None) == ""


def test_plan_preserves_ids_and_replaces_renamed_or_obsolete_indications():
    existing = [
        {"id": 1, "code_patho": 1, "code_classe_clinique": 10, "nom": "Asthme", "definition": "Old", "cis": []},
        {"id": 2, "code_patho": 2, "code_classe_clinique": None, "nom": "Old name", "definition": None, "cis": []},
        {"id": 3, "code_patho": None, "code_classe_clinique": 20, "nom": "Old class", "definition": None, "cis": []},
        {"id": 4, "code_patho": None, "code_classe_clinique": 30, "nom": "Linked class", "definition": None, "cis": []},
        {"id": 5, "code_patho": None, "code_classe_clinique": 99, "nom": "Obsolete", "definition": None, "cis": []},
    ]
    definitions = [
        {"code_patho": 1, "code_classe_clinique": 10, "definition": "Asthma definition"},
        {"code_patho": None, "code_classe_clinique": 20, "definition": "Class definition"},
        {"code_patho": 3, "code_classe_clinique": 30, "definition": "Path-specific definition"},
    ]
    pathologies = [
        {"code": 1, "nom": "Asthme", "cis": ["111", "222"]},
        {"code": 2, "nom": "New name", "cis": ["333"]},
    ]
    clinical_classes = [
        {"code": 20, "libelle_court": "DOULEUR", "cis": ["444"]},
        {"code": 30, "libelle_court": "CLASSE LIEE", "cis": ["555"]},
    ]

    inserted, updated, deleted = _plan_indications(existing, definitions, pathologies, clinical_classes)

    assert inserted == [
        {
            "code_patho": 2,
            "code_classe_clinique": None,
            "nom": "New name",
            "definition": None,
            "cis": ["333"],
        }
    ]
    assert updated == [
        {
            "id": 1,
            "code_patho": 1,
            "code_classe_clinique": 10,
            "nom": "Asthme",
            "definition": "Asthma definition",
            "cis": ["111", "222"],
        },
        {
            "id": 3,
            "code_patho": None,
            "code_classe_clinique": 20,
            "nom": "Douleur",
            "definition": "Class definition",
            "cis": ["444"],
        },
    ]
    assert deleted == [2, 4, 5]


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return iter(self.rows)


def test_build_indications_reads_sources_and_writes_plan_atomically(monkeypatch):
    existing = [{"id": 1, "code_patho": 1, "code_classe_clinique": None, "nom": "Old", "definition": None, "cis": []}]
    definitions = []
    pathologies = [{"code": 1, "nom": "New", "cis": ["111"]}]
    clinical_classes = [{"code": 20, "libelle_court": "DOULEUR", "cis": ["111"]}]
    writes = []

    class Connection:
        def execute(self, statement, params=None):
            sql = str(statement)
            if "FROM indications ORDER BY" in sql:
                return _Result(existing)
            if "FROM ref_pathologies" in sql:
                return _Result(definitions)
            if "FROM ansm_pathologie" in sql:
                return _Result(pathologies)
            if "FROM ansm_classe_clinique clinical_class" in sql:
                return _Result(clinical_classes)
            writes.append((sql, params))
            return _Result([])

    conn = Connection()
    engine = MagicMock()
    engine.begin.return_value = nullcontext(conn)
    monkeypatch.setattr(indications, "get_postgres_engine", lambda config: engine)

    result = build_indications("postgres-config")

    assert result == indications.IndicationBuildResult(inserted=2, updated=0, deleted=1)
    assert len(writes) == 2
    assert writes[0][0].startswith("INSERT INTO indications")
    assert writes[1][0].startswith("DELETE FROM indications")
    engine.begin.assert_called_once()
