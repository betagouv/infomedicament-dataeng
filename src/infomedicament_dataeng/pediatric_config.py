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
TIE_BREAKER_PRIORITY = {
    "AB": "AB",
    "AC": "AC",
    "BC": "B",
    "ABC": "AB"
}

# Keywords and matching

PEDIATRIC_KEYWORDS = [
    "pédiatrie", "pédiatrique", "enfant", "enfants",
    "nourrisson", "nourrissons",
    "nouveau-né", "nouveau-nés", "nouveaux-nés",
    "prématuré", "prématurés",
    "infantile",
    "adolescent", "adolescents", "adolescente", "adolescentes",
    "juvénile", "juvéniles",
    "immature",
]

# Patterns for age/weight mentions (< 18 years)
PEDIATRIC_AGE_PATTERNS = [
    # Age in years (0-18 ans): "âgé de moins de 12 ans", "< 6 ans", ">= 6 ans", etc.
    r'\b(?:âgée?s?|age|âge)\s*(?:de\s*)?(?:moins\s*de\s*|[<>]=?\s*|inférieure?\s*à\s*|supérieure?\s*à\s*)?(?:1[0-8]|[0-9])\s*ans?\b',
    # Age in months/days: any number is pediatric — "18 mois", "24 mois", "28 jours"
    r'\b(?:âgée?s?|age|âge)\s*(?:de\s*)?(?:moins\s*de\s*|[<>]=?\s*|inférieure?\s*à\s*|supérieure?\s*à\s*)?(?:[0-9]+)\s*(?:mois|jours?)\b',
    # "plus de 15 ans", "à partir de 16 ans" (age-bounded indications)
    r'\bplus\s*de\s*(?:1[0-7]|[0-9])\s*ans\b',
    r'\bà\s*partir\s*de\s*(?:1[0-7]|[0-9])\s*ans\b',
    # "poids < 30 kg", "poids >= 40 kg", "pesant moins de 15 kg"
    r'\b(?:poids|pesant)\s*(?:de\s*)?(?:moins\s*de\s*|[<>]=?\s*|inférieure?\s*à\s*|supérieure?\s*à\s*)?(?:[0-9]+(?:[.,][0-9]+)?)\s*kg\b',
]

# --- Negative phrase patterns (lead to C: "Sur avis") ---

NEGATIVE_PATTERNS = [
    r"ne doit pas être utilisée?",
    r"ne doivent pas être utilisée?s?",
    r"n'est pas indiquée?",
    r"ne sont pas indiquée?s?",
    r"n'est pas recommandée?",
    r"ne sont pas recommandée?s?",
    r"pas recommandable",
    r"sécurité.*?efficacité.*?n'ont pas été",
    r"sécurité.*?efficacité.*?n'a pas été",
    r"sécurité.*?efficacité.*?n'a\s*/\s*n'ont pas été",
    r"tolérance.*?efficacité.*?n'ont pas été",
    r"tolérance.*?efficacité.*?n'a pas été",
    r"n'a pas été suffisamment démontrée?",
    r"n'a pas été étudiée?",
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
]

ADULT_RESERVED_PATTERNS = [
    r"réservée?s?\s+à\s+l'adulte",
    r"réservée?s?\s+à\s+l\s+adulte",
    r"reservée?s?\s+a\s+l'adulte",
]

# Subsection titles that are headings but not specific content to match
_HEADING_ONLY_TITLES = {
    "population pédiatrique",
    "populations particulières",
    "posologie",
    "mode d'administration",
    "durée du traitement",
}