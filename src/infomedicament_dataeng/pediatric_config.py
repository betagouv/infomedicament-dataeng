"""Feature flags for pediatric classification logic.

Toggle these to switch classification behavior without changing code.
"""

# When True, a pediatric keyword in 4.1/4.2 must be accompanied by an
# explicit indication phrase (e.g. "est indiqué") to count as condition A.
# When False, any keyword match without a negative pattern counts as A.
REQUIRE_POSITIVE_INDICATION = False

POSITIVE_INDICATION_PATTERNS = [
    r"(?:est|sont)\s+indiquée?s?",
]

# This helps us decide how to handle cases where more than 1 conditions are met.
TIE_BREAKER_PRIORITY = {"AB": "AB", "AC": "A", "BC": "B", "ABC": "AB"}

# Keywords and matching

PEDIATRIC_KEYWORDS = [
    "pédiatrie",  # <18ans
    "pédiatrique",  # <18ans
    "enfant",  # <18ans
    "enfants",  # <18ans
    "nourrisson",  # 28 jours ou 1 mois <> 3 ans (?)
    "nourrissons",  # 28 jours ou 1 mois <> 3 ans (?)
    "nouveau-né",  # 28 jours
    "nouveau-nés",  # 28 jours
    "nouveaux-nés",  # 28 jours
    "prématuré",  # 28 jours
    "prématurés",  # 28 jours
    "infantile",  # 1 mois à 2ans/24mois
    "adolescent",  # 12ans <> 17 ans
    "adolescents",  # 12ans <> 17 ans
    "adolescente",  # 12ans <> 17 ans
    "adolescentes",  # 12ans <> 17 ans
    "juvénile",  # <18ans
    "juvéniles",  # <18ans
]

# Patterns for age/weight mentions (< 18 years)
PEDIATRIC_AGE_PATTERNS = [
    # Age in years (0-18 ans): "âgé de moins de 12 ans", "< 6 ans", ">= 6 ans", etc.
    r"\b(?:âgée?s?|age|âge)\s*(?:de\s*)?(?:moins\s*de\s*|[<>]=?\s*|inférieure?\s*à\s*|supérieure?\s*à\s*)?(?:1[0-8]|[0-9])\s*ans?\b",
    # Age in months/days: any number is pediatric — "18 mois", "24 mois", "28 jours"
    r"\b(?:âgée?s?|age|âge)\s*(?:de\s*)?(?:moins\s*de\s*|[<>]=?\s*|inférieure?\s*à\s*|supérieure?\s*à\s*)?(?:[0-9]+)\s*(?:mois|jours?)\b",
    # "plus de 15 ans", "à partir de 16 ans" (age-bounded indications)
    r"\bplus\s*de\s*(?:1[0-7]|[0-9])\s*ans\b",
    r"\bà\s*partir\s*de\s*(?:1[0-7]|[0-9])\s*ans\b",
    # "poids < 30 kg", "poids >= 40 kg", "pesant moins de 15 kg"
    r"\b(?:poids|pesant)\s*(?:de\s*)?(?:moins\s*de\s*|[<>]=?\s*|inférieure?\s*à\s*|supérieure?\s*à\s*)?(?:[0-9]+(?:[.,][0-9]+)?)\s*kg\b",
]

# --- Negative phrase patterns (lead to C: "Sur avis") ---

NEGATIVE_PATTERNS = [
    r"ne doit pas être utilisée?",
    r"ne doivent pas être utilisée?s?",
    r"n'est pas indiquée?",
    r"ne sont pas indiquée?s?",
    r"n'est (?:\w+\s+)?pas recommandée?",
    r"ne sont (?:\w+\s+)?pas recommandée?s?",
    r"pas recommandable",
    r"efficacité.*?sécurité.*?n'ont pas (?:encore )?été",
    r"efficacité.*?sécurité.*?n'a pas (?:encore )?été",
    r"efficacité.*?sécurité.*?n'a\s*/\s*n'ont pas (?:encore )?été",
    r"sécurité.*?efficacité.*?n'ont pas (?:encore )?été",
    r"sécurité.*?efficacité.*?n'a pas (?:encore )?été",
    r"sécurité.*?efficacité.*?n'a\s*/\s*n'ont pas (?:encore )?été",
    r"tolérance.*?efficacité.*?n'ont pas (?:encore )?été",
    r"tolérance.*?efficacité.*?n'a pas (?:encore )?été",
    r"n'a pas (?:encore )?été suffisamment démontrée?",
    r"n'a pas (?:encore )?été étudiée?",
    r"n'est pas justifiée?",
    r"il n'existe pas d'utilisation justifiée?",
    r"est déconseillée?",
    r"aucune donnée.*?disponible",
    r"aucune étude.*?effectuée",
    r"données disponibles sont limitées",
    r"peu de données",
    r"pas possible de recommander",
    r"en l'absence de données?",
    r"absence d'expérience",
    r"sans objet",
    r"est contre-indiquée?",
    r"il existe d'autres formes",
    r"n'ont pas permis de démontrer",
    r"ne soutiennent pas son utilisation",
]

ADULT_RESERVED_PATTERNS = [
    r"réservée?s?\s+à\s+l'adulte",
    r"réservée?s?\s+à\s+l\s+adulte",
    r"reservée?s?\s+a\s+l'adulte",
]

# Implicit age range (min_year, max_year) for each keyword, used when no explicit
# age range is present in the text. Age 0 = neonate/newborn.
KEYWORD_AGE_RANGES: dict[str, tuple[int, int]] = {
    "pédiatrie": (0, 17),
    "pédiatrique": (0, 17),
    "enfant": (0, 17),
    "enfants": (0, 17),
    "juvénile": (0, 17),
    "juvéniles": (0, 17),
    "nourrisson": (0, 2),
    "nourrissons": (0, 2),
    "infantile": (0, 2),
    "nouveau-né": (0, 0),
    "nouveau-nés": (0, 0),
    "nouveaux-nés": (0, 0),
    "prématuré": (0, 0),
    "prématurés": (0, 0),
    "adolescent": (12, 17),
    "adolescents": (12, 17),
    "adolescente": (12, 17),
    "adolescentes": (12, 17),
}

# Subsection titles that are headings but not specific content to match.
# Matched case-insensitively against the full stripped heading text.
_HEADING_ONLY_TITLE_PATTERNS = [
    r"populations?\s+pédiatriques?",
    r"populations?\s+particulières?",
    r"posologie",
    r"mode\s+d['']administration",
    r"durée\s+du\s+traitement",
    # Age/weight sub-group headings that carry no classification signal on their own
    r"enfants?\s+(?:âgés?\s+)?de\s+moins\s+de\s+\d+\s+ans(?:\s+(?:et\s+de\s+moins\s+de|pesant\s+(?:au\s+moins|moins\s+de))\s+\d+\s+kg)?",
    r"enfants?\s+de\s+\d+\s+à\s+\d+\s+ans(?:\s+pesant\s+(?:au\s+moins|moins\s+de)\s+\d+\s+kg)?",
    r"adultes?\s+et\s+adolescents?\s*\(\d+\s+ans\s+et\s+plus\)",
    r"enfants?\s+et\s+adolescents?\s*\(\d+(?:[-–]|\s+à\s+)\d+\s+ans\)",
    # Standalone pediatric keyword headings with no clinical sentence
    r"(?:nouveau-nés?|nouveaux-nés?|nourrissons?|prématurés?|enfants?|adolescentes?s?|pédiatrie|pédiatrique|infantile|juvéniles?)",
]
