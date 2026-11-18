"""Regression cases from infomedicament PR #287's title helpers."""

import pytest
from sqlalchemy import create_engine, text

from infomedicament_dataeng.resume.titles import load_titles_by_subs_id, page_title


def test_titles_use_all_catalog_names_and_composition_fallbacks():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE ansm_substance_nom (code_substance TEXT, code_nom TEXT, nom TEXT, type TEXT)"))
        conn.execute(text("CREATE TABLE ansm_composant (code_substance TEXT, substance TEXT)"))
        conn.execute(
            text("INSERT INTO ansm_substance_nom VALUES (:subs, :id, :name, :type)"),
            [
                dict(subs=" 31844 ", id="65799", name="abacavir", type="SYNONYME"),
                dict(subs="31844", id="31844", name=" abacavir base ", type="CANONIQUE"),
                dict(subs="02725", id="02725", name="(NE PAS UTILISER)- LEVOGLUTAMIDE", type="CANONIQUE"),
                dict(subs="02725", id="72547", name="glutamine", type="SYNONYME"),
                dict(subs="96983", id="91833", name="virus de la varicelle", type="SYNONYME"),
                dict(subs="96983", id="10000", name="varicelle", type="SYNONYME"),
                dict(subs="numeric", id=" 10 ", name="ten", type=None),
                dict(subs="numeric", id=" 2 ", name="two", type=None),
                dict(subs="deprecated", id="1", name="(NE PAS UTILISER) old", type="CANONIQUE"),
                dict(subs="empty", id="1", name=None, type="CANONIQUE"),
                # A name ID is never resolved as a substance ID.
                dict(subs="other", id="00140", name="other substance", type="CANONIQUE"),
                dict(subs="entities", id="1", name=" &#xE9;ther&nbsp; &Delta;  ", type="CANONIQUE"),
            ],
        )
        conn.execute(
            text("INSERT INTO ansm_composant VALUES (:subs, :name)"),
            [
                dict(subs="63787", name="z label"),
                dict(subs=" 63787 ", name="estradiol anhydre ((cell source))"),
                dict(subs="63787", name=" "),
                dict(subs="missing", name=None),
                dict(subs="empty", name="must not replace an existing catalog name"),
                dict(subs="31844", name="must not replace the canonical name"),
            ],
        )

        titles = load_titles_by_subs_id(conn)

    assert titles == {
        "31844": "abacavir base",
        "02725": "glutamine",
        "96983": "varicelle",
        "numeric": "two",
        "deprecated": "(NE PAS UTILISER) old",
        "empty": "",
        "other": "other substance",
        "entities": "éther Δ",
        "63787": "estradiol anhydre",
    }
    assert page_title(["31844", "00140"], titles) is None


@pytest.mark.parametrize(
    ("ids", "expected"),
    [
        ([" 02202 ", "74765"], "paracétamol, codéine (phosphate de) hémihydraté"),
        (["74765", "02202"], "codéine (phosphate de) hémihydraté, paracétamol"),
        (["02202", "02202"], "paracétamol, paracétamol"),
        (["02202", "missing"], None),
        ([], None),
    ],
)
def test_page_titles_preserve_component_order_and_duplicates(ids, expected):
    assert page_title(ids, {"02202": "paracétamol", "74765": "codéine (phosphate de) hémihydraté"}) == expected
