"""File I/O operations for HTML documents."""

import chardet


def charger_html(fichier_html: str) -> str:
    """Load an HTML file with automatic encoding detection."""
    with open(fichier_html, "rb") as f:
        contenu_binaire = f.read()
        encodage = chardet.detect(contenu_binaire)["encoding"] or "utf-8"
    return contenu_binaire.decode(encodage)


def charger_liste_cis(fichier_cis: str) -> set[str]:
    """Load the list of CIS codes from a text or CSV file.

    Supports plain text (one CIS per line) and CSV files (takes the first
    column, skips header if present).
    """
    cis_autorises = set()
    with open(fichier_cis, "r", encoding="utf-8") as f:
        for ligne in f:
            # Take first field (handles both plain text and CSV)
            cis = ligne.strip().split(",")[0].strip()
            if cis and cis.isdigit():
                cis_autorises.add(cis)
    return cis_autorises
