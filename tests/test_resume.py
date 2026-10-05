"""Tests for modular resume-table builders."""

from contextlib import nullcontext
from unittest.mock import MagicMock

import pytest

from infomedicament_dataeng.resume import builder, composants, medicines, specialties, substances
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
    element=1,
):
    return Composant(
        cis="1",
        element_number=element,
        element_order=element,
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
    substance = make_composant(number=1, nature="substance")
    fraction = make_composant(number=1, nature="fraction")
    other = make_composant(number=2, nature="substance")

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


@pytest.mark.parametrize("main_names", [{"00005": "acétylsalicylique (acide)"}, {}])
def test_specialties_omits_main_names_when_names_match_or_canonical_is_missing(monkeypatch, main_names):
    composant = make_composant()
    rows = []
    monkeypatch.setattr(specialties, "load_specialties", lambda conn: [Specialty("1", "TEST 1 mg", "N", "DISPONIBLE")])
    monkeypatch.setattr(specialties, "load_composants_by_cis", lambda conn: {"1": [composant]})
    monkeypatch.setattr(specialties, "load_main_names_by_subs_id", lambda conn: main_names)
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
    monkeypatch.setattr(
        substances, "load_name_types_by_nom_id", lambda conn: {"00005": "CANONIQUE", "34911": "SYNONYME"}
    )
    monkeypatch.setattr(substances, "replace_rows", lambda conn, table, values: rows.extend(values))
    monkeypatch.setattr(substances, "replace_letters", lambda *args: None)

    substances.build(None)

    assert rows == [
        {
            "SubsId": "00005",
            "NomId": "00005",
            "NomLib": "acétylsalicylique (acide)",
            "type": "CANONIQUE",
            "specialites": 2,
        },
        {
            "SubsId": "00005",
            "NomId": "34911",
            "NomLib": "acide acétylsalicylique",
            "type": "SYNONYME",
            "specialites": 2,
        },
    ]


def test_canonical_lookup_uses_source_type_and_trims_ids():
    conn = MagicMock()
    conn.execute.return_value.mappings.return_value = [
        {"code_substance": " 00123 ", "code_nom": " 98765 ", "nom": " canonical label ", "type": "CANONIQUE"},
        {"code_substance": "00123", "code_nom": "00123", "nom": "alias", "type": "SYNONYME"},
        {"code_substance": "00124", "code_nom": "00124", "nom": "untyped", "type": None},
    ]

    assert composants.load_main_names_by_subs_id(conn) == {"00123": "canonical label"}
    assert composants.load_name_types_by_nom_id(conn) == {"98765": "CANONIQUE", "00123": "SYNONYME", "00124": None}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  alpha&nbsp;&Delta;   substance  ", "alpha Δ substance"),
        ("&#xE9;&#233;\t&#32;test", "éé test"),
        ("&amp;&apos;&quot;&lt;&gt;&lsquo;&rsquo;", "&'\"<>‘’"),
        ("&unknown; &delta; &AMP;", "&unknown; &delta; &AMP;"),
        ("&#x26;nbsp;\n&#38;Delta;", "Δ"),
    ],
)
def test_clean_substance_name(raw, expected):
    assert substances.clean_substance_name(raw) == expected


def test_substance_rows_use_cleaned_names_for_letters_and_nullable_type(monkeypatch):
    rows = []
    letters = []
    monkeypatch.setattr(substances, "load_specialties", lambda conn: [Specialty("1", "TEST 1 mg", "N", "DISPONIBLE")])
    monkeypatch.setattr(
        substances, "load_composants_by_cis", lambda conn: {"1": [make_composant(name="  &#xE9;ther&nbsp;  Δ  ")]}
    )
    monkeypatch.setattr(substances, "load_name_types_by_nom_id", lambda conn: {})
    monkeypatch.setattr(substances, "replace_rows", lambda conn, table, values: rows.extend(values))
    monkeypatch.setattr(substances, "replace_letters", lambda conn, target, values: letters.append(values))

    substances.build(None)

    assert rows[0]["NomLib"] == "éther Δ"
    assert rows[0]["type"] is None
    assert letters == [{"E"}]


def test_medicine_specialty_entries_have_independent_ordered_compositions(monkeypatch):
    first = make_composant(substance_id=" X ", name_id=" X_NAME ", name="substance X")
    hidden = make_composant(substance_id="Y", name_id="BASE", name="hidden base", number=2)
    fraction = make_composant(substance_id="Y", name_id="Y_NAME", name="substance Y", number=2, nature="fraction")
    repeated = make_composant(substance_id="X", name_id="X_OTHER", name="other X", element=2)
    rows = []
    monkeypatch.setattr(
        medicines,
        "load_specialties",
        lambda conn: [
            Specialty("1", "TEST 1 mg", "N", "DISPONIBLE"),
            Specialty("2", "TEST 2 mg", "C", "ALERTE"),
            Specialty("3", "TEST 3 mg", "N", "PARTIELLE"),
        ],
    )
    monkeypatch.setattr(
        medicines, "load_composants_by_cis", lambda conn: {"1": [first], "2": [first, hidden, fraction, repeated]}
    )
    monkeypatch.setattr(medicines, "load_indications", lambda conn: [])
    monkeypatch.setattr(medicines, "load_atc_by_cis", lambda conn: {})
    monkeypatch.setattr(medicines, "load_surveillance_cis", lambda conn: {"2"})
    monkeypatch.setattr(medicines, "replace_rows", lambda conn, table, values: rows.extend(values))
    monkeypatch.setattr(medicines, "replace_letters", lambda *args: None)

    medicines.build(None)

    assert len(rows) == 1
    assert rows[0]["specialites"] == [
        ["1", "TEST 1 mg", "1", "N", "false", "substance X", "X", "X_NAME"],
        ["2", "TEST 2 mg", "3", "C", "true", "substance X, substance Y, other X", "X,Y,X", "X_NAME,Y_NAME,X_OTHER"],
        ["3", "TEST 3 mg", "2", "N", "false", "", "", ""],
    ]
    assert rows[0]["composants"] == "substance X"
    assert rows[0]["subsIds"] == ["X"]
    assert rows[0]["subsNamesIds"] == ["X_NAME"]


def test_component_selection_keeps_other_kit_elements_and_source_order():
    base = make_composant()
    other_element = make_composant(element=2)
    fraction = make_composant(nature="fraction")
    repeated = make_composant(element=2, number=2)

    assert composants.simple_composants([base, other_element, fraction, repeated]) == [
        other_element,
        fraction,
        repeated,
    ]


def test_component_mapping_cleans_display_without_changing_source():
    conn = MagicMock()
    raw = {
        "cis": "1",
        "numero_element": 1,
        "numero_composant": 1,
        "code_substance": " 00123 ",
        "substance": " substance name ((cell source)) ",
        "nature": "Substance active",
        "ordre": 1,
    }
    conn.execute.return_value.mappings.side_effect = [
        [raw],
        [{"code_substance": " 00123 ", "code_nom": " 98765 ", "nom": "canonical", "type": "CANONIQUE"}],
        [{"cis": "1", "numero_element": 1, "ordre": 1}],
    ]

    mapped = composants.load_composants_by_cis(conn)["1"][0]

    assert mapped.name == "substance name"
    assert mapped.name_id == "98765"
    assert raw["substance"] == " substance name ((cell source)) "
    assert composants.clean_component_display_name(" name (salt) ") == "name (salt)"
    assert composants.clean_component_display_name(" name ((nested (source))) ") == "name ((nested (source)))"


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
