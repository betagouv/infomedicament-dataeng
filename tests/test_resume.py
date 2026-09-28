"""Tests for modular resume-table builders."""

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from infomedicament_dataeng.resume import builder, medicines, specialties, substances
from infomedicament_dataeng.resume.common import (
    Composant,
    Specialty,
    format_name,
    generic_dci,
    group_name,
    normalize_letter,
    normalize_numeric_id,
    replace_rows,
    sorted_letters,
)
from infomedicament_dataeng.resume.composants import simple_composants


def make_composant(
    *,
    substance_id="00005",
    name_id="00005",
    name="acétylsalicylique (acide)",
    nature="substance",
    number=1,
):
    return Composant(
        cis="1",
        element_number=1,
        element_order=1,
        nature=nature,
        composant_number=number,
        composant_order=number,
        substance_id=substance_id,
        name_id=name_id,
        name=name,
    )


def test_specialty_compatibility_fields():
    assert Specialty("1", "Name", "NATIONALE", "DISPONIBLE").status == 1
    assert Specialty("1", "Name", "NATIONALE", "PARTIELLE").status == 2
    assert Specialty("1", "Name", "NATIONALE", "ALERTE").status == 3


@pytest.mark.parametrize(
    ("denomination", "expected"),
    [
        ("ABACAVIR ARROW 300 mg, comprimé", "ABACAVIR ARROW"),
        ("A 313 50 000 U.I.", "A"),
        ("MEDICAMENT, comprimé", "MEDICAMENT"),
        ("123 TEST", "123 TEST"),
    ],
)
def test_group_name_matches_application(denomination, expected):
    assert group_name(denomination) == expected


def test_generic_name_formatting_matches_application():
    assert generic_dci("IRBESARTAN 150 mg - GROUPE") == "IRBESARTAN 150 mg"
    assert format_name("IRBESARTAN 150 mg") == "Irbesartan 150 mg"
    assert format_name("ÉTANERCEPT") == "ÉTANERCEPT"


def test_letter_normalization_and_sorting_match_navigation():
    assert normalize_letter("é") == "E"
    assert sorted_letters({"A", "6", "["}) == ["[", "6", "A"]


def test_numeric_alert_id_normalization():
    assert normalize_numeric_id(" 004179 ") == "4179"
    assert normalize_numeric_id("000") is None
    assert normalize_numeric_id("A123") is None


def test_simple_composants_prefers_fractions_per_composant_number():
    substance = SimpleNamespace(composant_number=1, nature="substance")
    fraction = SimpleNamespace(composant_number=1, nature="fraction")
    other = SimpleNamespace(composant_number=2, nature="substance")

    assert simple_composants([substance, fraction, other]) == [fraction, other]


def test_medicines_uses_selected_composants_for_aligned_substance_ids(monkeypatch):
    selected = make_composant(name_id="34911", name="acide acétylsalicylique", nature="fraction")
    base = make_composant(name="base hidden by fraction")
    other = make_composant(substance_id="00006", name_id="00007", name="caféine", number=2)
    rows = []
    monkeypatch.setattr(medicines, "load_specialties", lambda conn: [Specialty("1", "TEST 1 mg", "N", "DISPONIBLE")])
    monkeypatch.setattr(medicines, "load_composants_by_cis", lambda conn: {"1": [base, selected, other]})
    monkeypatch.setattr(medicines, "load_indications", lambda conn: [])
    monkeypatch.setattr(medicines, "load_atc_by_cis", lambda conn: {})
    monkeypatch.setattr(medicines, "load_surveillance_cis", lambda conn: set())
    monkeypatch.setattr(medicines, "replace_rows", lambda conn, table, values: rows.extend(values))
    monkeypatch.setattr(medicines, "replace_letters", lambda *args: None)

    medicines.build(None)

    assert rows[0]["composants"] == "acide acétylsalicylique, caféine"
    assert rows[0]["subsIds"] == ["00005", "00006"]
    assert rows[0]["subsNamesIds"] == ["34911", "00007"]


def test_specialties_writes_all_main_names_when_one_name_is_secondary(monkeypatch):
    secondary = make_composant(name_id="34911", name="acide acétylsalicylique")
    canonical = make_composant(substance_id="00006", name_id="00006", name="caféine", number=2)
    rows = []
    monkeypatch.setattr(specialties, "load_specialties", lambda conn: [Specialty("1", "TEST 1 mg", "N", "DISPONIBLE")])
    monkeypatch.setattr(specialties, "load_composants_by_cis", lambda conn: {"1": [secondary, canonical]})
    monkeypatch.setattr(
        specialties,
        "load_main_names_by_subs_id",
        lambda conn: {"00005": "acétylsalicylique (acide)", "00006": "caféine"},
    )
    monkeypatch.setattr(specialties, "load_indications", lambda conn: [])
    monkeypatch.setattr(specialties, "load_atc_by_cis", lambda conn: {})
    monkeypatch.setattr(specialties, "load_surveillance_cis", lambda conn: set())
    monkeypatch.setattr(specialties, "_load_alerts", lambda conn: (set(), set(), {}))
    monkeypatch.setattr(specialties, "replace_rows", lambda conn, table, values: rows.extend(values))

    specialties.build(None)

    assert rows[0]["subsIds"] == ["00005", "00006"]
    assert rows[0]["subsMainNames"] == "acétylsalicylique (acide), caféine"


def test_specialties_omits_main_names_when_all_names_are_canonical(monkeypatch):
    composant = make_composant()
    rows = []
    monkeypatch.setattr(specialties, "load_specialties", lambda conn: [Specialty("1", "TEST 1 mg", "N", "DISPONIBLE")])
    monkeypatch.setattr(specialties, "load_composants_by_cis", lambda conn: {"1": [composant]})
    monkeypatch.setattr(specialties, "load_main_names_by_subs_id", lambda conn: {"00005": "acétylsalicylique (acide)"})
    monkeypatch.setattr(specialties, "load_indications", lambda conn: [])
    monkeypatch.setattr(specialties, "load_atc_by_cis", lambda conn: {})
    monkeypatch.setattr(specialties, "load_surveillance_cis", lambda conn: set())
    monkeypatch.setattr(specialties, "_load_alerts", lambda conn: (set(), set(), {}))
    monkeypatch.setattr(specialties, "replace_rows", lambda conn, table, values: rows.extend(values))

    specialties.build(None)

    assert rows[0]["subsMainNames"] is None


def test_substances_aggregates_speciality_counts_by_substance_id(monkeypatch):
    canonical = make_composant()
    secondary = make_composant(name_id="34911", name="acide acétylsalicylique")
    rows = []
    monkeypatch.setattr(
        substances,
        "load_specialties",
        lambda conn: [
            Specialty("1", "FIRST 1 mg", "N", "DISPONIBLE"),
            Specialty("2", "SECOND 1 mg", "N", "DISPONIBLE"),
        ],
    )
    monkeypatch.setattr(substances, "load_composants_by_cis", lambda conn: {"1": [canonical], "2": [secondary]})
    monkeypatch.setattr(substances, "replace_rows", lambda conn, table, values: rows.extend(values))
    monkeypatch.setattr(substances, "replace_letters", lambda *args: None)

    substances.build(None)

    assert rows == [
        {"SubsId": "00005", "NomId": "00005", "NomLib": "acétylsalicylique (acide)", "specialites": 2},
        {"SubsId": "00005", "NomId": "34911", "NomLib": "acide acétylsalicylique", "specialites": 2},
    ]


def test_replace_rows_quotes_application_column_names():
    conn = MagicMock()

    replace_rows(conn, "resume_specialites", [{"specId": "1", "ProcId": "NATIONALE"}])

    assert str(conn.execute.call_args_list[0].args[0]) == "DELETE FROM resume_specialites"
    assert (
        str(conn.execute.call_args_list[1].args[0])
        == 'INSERT INTO resume_specialites ("specId", "ProcId") VALUES (:specId, :ProcId)'
    )


def test_orchestrator_runs_each_target_in_its_own_transaction(monkeypatch):
    calls = []
    engine = MagicMock()
    engine.begin.side_effect = [nullcontext("conn-1"), nullcontext("conn-2")]
    monkeypatch.setattr(builder, "get_postgres_engine", lambda config: engine)
    monkeypatch.setattr(
        builder,
        "BUILDERS",
        {
            "one": lambda conn: calls.append(("one", conn)) or 1,
            "two": lambda conn: calls.append(("two", conn)) or 2,
        },
    )

    assert builder.build_resume("all", "postgres-config") == {"one": 1, "two": 2}
    assert calls == [("one", "conn-1"), ("two", "conn-2")]


def test_orchestrator_rejects_unknown_target():
    with pytest.raises(ValueError, match="Unknown resume target"):
        builder.build_resume("unknown")
