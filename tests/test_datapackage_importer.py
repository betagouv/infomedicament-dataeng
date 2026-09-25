"""Tests for ANSM datapackage import configuration."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from infomedicament_dataeng.datapackage_importer import LOAD_ORDER, _load_resource


def test_load_order_covers_current_ansm_package_resources():
    expected_resources = {
        "specialite",
        "presentation",
        "presentation_evenement",
        "element",
        "recipient",
        "atc",
        "specialite_atc",
        "classe_clinique",
        "specialite_classe_clinique",
        "pathologie",
        "classe_clinique_pathologie",
        "delivrance",
        "specialite_delivrance",
        "specialite_titulaire",
        "specialite_evenement",
        "caracteristique",
        "composant",
        "substance_nom",
        "dispositif",
        "groupe_substance",
        "classe_interaction",
        "substance_groupe_substance",
        "classe_groupe_substance",
        "interaction",
        "groupe_generique",
        "specialite_groupe_generique",
        "document",
        "excipient_effet_notoire",
        "specialite_excipient_effet_notoire",
    }

    assert len(LOAD_ORDER) == len(set(LOAD_ORDER))
    assert set(LOAD_ORDER) == expected_resources


def test_load_order_places_new_resources_after_their_parents():
    positions = {resource: index for index, resource in enumerate(LOAD_ORDER)}
    dependencies = [
        ("presentation_evenement", "presentation"),
        ("classe_clinique_pathologie", "classe_clinique"),
        ("classe_clinique_pathologie", "pathologie"),
        ("specialite_delivrance", "specialite"),
        ("specialite_delivrance", "delivrance"),
        ("specialite_evenement", "specialite"),
        ("groupe_generique", "atc"),
        ("specialite_groupe_generique", "groupe_generique"),
        ("specialite_groupe_generique", "specialite"),
        ("specialite_excipient_effet_notoire", "excipient_effet_notoire"),
        ("specialite_excipient_effet_notoire", "specialite"),
    ]

    for child, parent in dependencies:
        assert positions[parent] < positions[child]


def test_specialty_resource_synchronizes_metadata_when_empty():
    package = SimpleNamespace(get_resource=lambda name: SimpleNamespace(read_rows=lambda: []))
    conn = MagicMock()
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False

    with patch("infomedicament_dataeng.datapackage_importer.sync_specialites_metadata") as sync_metadata:
        assert _load_resource(package, "specialite", "ansm_specialite", engine) == 0

    sync_metadata.assert_called_once_with(conn)
