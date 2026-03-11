"""Pytest configuration and fixtures."""

from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class FakeCursor:
    """Minimal fake psycopg2 cursor for testing."""

    def __init__(self, ids=()):
        self._ids = iter(ids)
        self.execute_calls = []

    def execute(self, _sql, params=None):
        self.execute_calls.append(params)

    def fetchone(self):
        return (next(self._ids),)


@pytest.fixture
def fake_cursor():
    return FakeCursor


@pytest.fixture
def sample_notice_html() -> str:
    """Load sample Notice HTML file."""
    sample_file = FIXTURES_DIR / "N0314839.htm"
    return sample_file.read_bytes().decode("utf-8", errors="replace")


@pytest.fixture
def sample_atc_sql() -> Path:
    """Return the path to the sample ATC SQL file."""
    return FIXTURES_DIR / "ClasseATC_sample.sql"


@pytest.fixture
def tmp_csv(tmp_path: Path) -> Path:
    """Return a temporary path for CSV output."""
    return tmp_path / "output.csv"


def _make_rcp(cis="12345", sections=None):
    """Build a minimal RCP dict with given section texts.

    Args:
        cis: CIS code.
        sections: dict mapping section prefix (e.g. "4.1") to list of text strings.
    """
    sections = sections or {}
    titre2_nodes = []
    for prefix, texts in sections.items():
        children = [{"type": "AmmCorpsTexte", "content": t} for t in texts]
        titre2_nodes.append(
            {
                "type": "AmmAnnexeTitre2",
                "content": f"{prefix} Section heading",
                "children": children,
            }
        )
    return {
        "source": {"cis": cis},
        "content": [
            {"type": "AmmAnnexeTitre1", "children": titre2_nodes},
        ],
    }


@pytest.fixture
def make_rcp():
    """Factory fixture to build mock RCP dicts."""
    return _make_rcp


@pytest.fixture
def ground_truth_csv(tmp_path: Path) -> Path:
    """Write a small ground truth CSV and return its path."""
    gt_file = tmp_path / "gt.csv"
    gt_file.write_text(
        "CIS,A:Indication,B:Contre-indication,C:Sur avis\n12345,oui,non,non\n67890,non,non,oui\n",
        encoding="utf-8",
    )
    return gt_file
