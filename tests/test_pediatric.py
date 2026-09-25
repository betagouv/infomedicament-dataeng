"""Tests for PostgreSQL-backed pediatric classification."""

import csv

from infomedicament_dataeng import cli, db
from infomedicament_dataeng.pediatric import classify, extract_section_texts


def semantic_rcp(sections: dict[str, list[str]]) -> str:
    anchors = {
        "4.1": "RcpIndicTherap",
        "4.2": "RcpPosoAdmin",
        "4.3": "RcpContreindications",
        "4.4": "RcpMisesEnGarde",
    }
    parts = []
    for number, texts in sections.items():
        parts.append(f'<h3 id="{anchors[number]}">{number} Section</h3>')
        parts.extend(f"<p>{text}</p>" for text in texts)
    return "".join(parts)


def test_extract_section_texts_uses_semantic_heading_boundaries():
    content_html = semantic_rcp(
        {
            "4.1": ["Indiqué chez l'enfant"],
            "4.2": ["Posologie chez l'enfant"],
        }
    )

    assert extract_section_texts(content_html, "4.1") == ["Indiqué chez l'enfant"]
    assert extract_section_texts(content_html, "4.2") == ["Posologie chez l'enfant"]


def test_extract_section_texts_ignores_generic_subheading():
    content_html = (
        '<h3 id="RcpPosoAdmin">4.2 Posologie</h3>'
        "<h4>Population pédiatrique</h4>"
        "<p>Sans objet</p>"
        '<h3 id="RcpContreindications">4.3 Contre-indications</h3>'
    )

    assert extract_section_texts(content_html, "4.2") == ["Sans objet"]


def test_classify_positive_indication_from_semantic_html():
    result = classify("12345", semantic_rcp({"4.1": ["Ce médicament est indiqué chez l'enfant de plus de 6 ans"]}))

    assert result.cis == "12345"
    assert result.condition_a is True
    assert result.condition_b is False


def test_classify_negative_and_contraindication_mentions():
    result = classify(
        "12345",
        semantic_rcp(
            {
                "4.2": ["La sécurité et l'efficacité n'ont pas été étudiées chez les enfants"],
                "4.3": ["Contre-indiqué chez le nourrisson de moins de 3 mois"],
            }
        ),
    )

    assert result.condition_a is False
    assert result.condition_b is True
    assert result.condition_c is False  # B wins through the configured tie-breaker.


def test_classify_uses_postgresql_atc_code():
    result = classify("12345", semantic_rcp({"4.1": ["Contraception orale"]}), atc_code="G03AA07")

    assert result.condition_c is True
    assert "contraceptif (ATC G03)" in result.c_reasons


def test_iter_pediatric_rcps_streams_database_rows(monkeypatch):
    class FakeConnection:
        def __init__(self):
            self.params = None
            self.batch_size = None

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execution_options(self, *, yield_per):
            self.batch_size = yield_per
            return self

        def execute(self, query, params):
            self.params = params
            return self

        def mappings(self):
            return iter([{"cis": "12345", "content_html": "<p>RCP</p>", "atc_code": "A01"}])

    connection = FakeConnection()
    monkeypatch.setattr(
        db, "get_postgres_engine", lambda config: type("Engine", (), {"connect": lambda self: connection})()
    )

    records = list(db.iter_pediatric_rcps("config", cis="12345", limit=1, batch_size=25))

    assert records == [{"cis": "12345", "content_html": "<p>RCP</p>", "atc_code": "A01"}]
    assert connection.params == {"cis": "12345", "limit": 1}
    assert connection.batch_size == 25


def test_run_pediatric_classification_writes_predictions(tmp_path):
    output = tmp_path / "predictions.csv"
    records = [
        {
            "cis": "12345",
            "content_html": semantic_rcp({"4.1": ["Ce médicament est indiqué chez l'enfant"]}),
            "atc_code": "A01",
        }
    ]

    cli.run_pediatric_classification(records, None, str(output))

    rows = list(csv.DictReader(output.open(encoding="utf-8")))
    assert rows[0]["cis"] == "12345"
    assert rows[0]["pred_A"] == "1"
