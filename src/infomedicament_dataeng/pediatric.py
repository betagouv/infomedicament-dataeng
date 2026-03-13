"""Pediatric drug classification based on RCP section content.

Classifies medications as:
- A: Indication pédiatrique (pediatric indication exists)
- B: Contre-indication pédiatrique (pediatric contraindication exists)
- C: Sur avis d'un professionnel de santé (requires professional advice)

This classification process is fully deterministic and based on keyword
and pattern matching in the relevant RCP sections (4.1, 4.2 for A/C; 4.3 for B).
It does not use any machine learning or external data sources.
"""

import csv
import re
from dataclasses import dataclass, field

from infomedicament_dataeng import pediatric_config

# --- Section extraction ---


def extract_section_texts(rcp_json: dict, section_prefix: str) -> list[str]:
    """Extract all text blocks from an RCP section.

    Args:
        rcp_json: Parsed RCP JSON (with "content" key).
        section_prefix: Section number prefix, e.g. "4.1", "4.2", "4.3".

    Returns:
        List of text strings, one per text element found in the section.
    """
    texts: list[str] = []
    for item in rcp_json.get("content", []):
        # Look only for AmmAnnexeTitre1 nodes (level 1 sections)
        if item.get("type") != "AmmAnnexeTitre1":
            continue
        for child in item.get("children", []):
            if child.get("type") == "AmmAnnexeTitre2":
                heading = child.get("content", "").strip()
                if heading.startswith(section_prefix):
                    _collect_texts(child, texts)
                    return texts
    return texts


def _is_heading_label(text: str) -> bool:
    """Return True if text is a standalone structural heading with no clinical signal."""
    return any(re.fullmatch(p, text, re.IGNORECASE) for p in pediatric_config._HEADING_ONLY_TITLE_PATTERNS)


def _collect_texts(node: dict, texts: list[str]) -> None:
    """Recursively collect text content from a JSON node."""
    content = node.get("content", "")
    node_type = node.get("type", "")

    # Skip the section heading itself (AmmAnnexeTitre2)
    if node_type == "AmmAnnexeTitre2":
        pass
    elif isinstance(content, str) and content.strip() and not _is_heading_label(content.strip()):
        texts.append(content.strip())
    elif isinstance(content, list):
        # Bullet list items
        for item in content:
            if isinstance(item, str) and item.strip() and not _is_heading_label(item.strip()):
                texts.append(item.strip())

    for child in node.get("children", []):
        _collect_texts(child, texts)


# --- Classification ---


def find_pediatric_keywords_in_text(text: str) -> list[str]:
    """Find all pediatric keywords/patterns present in a text block."""
    if not text:
        return []
    text_lower = text.lower()
    found = []

    for kw in pediatric_config.PEDIATRIC_KEYWORDS:
        if kw in text_lower:
            found.append(kw)

    for pattern in pediatric_config.PEDIATRIC_AGE_PATTERNS:
        for match in re.finditer(pattern, text_lower, re.IGNORECASE):
            found.append(match.group())

    return list(dict.fromkeys(found))  # dedupe, preserve order


def matches_negative_pattern(text: str) -> str | None:
    """Check if text matches any negative phrase pattern.

    Returns the matched pattern string, or None.
    """
    text_lower = text.lower()
    for pattern in pediatric_config.NEGATIVE_PATTERNS:
        if re.search(pattern, text_lower):
            return pattern
    return None


def matches_positive_indication(text: str) -> bool:
    """Check if text contains an explicit indication phrase like 'est indiqué'."""
    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in pediatric_config.POSITIVE_INDICATION_PATTERNS)


def is_adult_reserved(text: str) -> bool:
    """Check if text contains a 'réservé à l'adulte' phrase."""
    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in pediatric_config.ADULT_RESERVED_PATTERNS)


@dataclass
class SentenceMatch:
    """A text block that matched during classification."""

    text: str
    keywords: list[str]
    negative_pattern: str | None = None
    is_positive: bool = False


@dataclass
class PediatricClassification:
    """Result of pediatric classification for a single drug."""

    cis: str
    condition_a: bool = False
    condition_b: bool = False
    condition_c: bool = False
    a_reasons: list[str] = field(default_factory=list)
    b_reasons: list[str] = field(default_factory=list)
    c_reasons: list[str] = field(default_factory=list)
    matches_41_42: list[SentenceMatch] = field(default_factory=list)
    matches_43: list[SentenceMatch] = field(default_factory=list)


# --- Age-based filtering ---

# Each tuple: (compiled regex, handler) where handler(m) -> (min_year, max_year)
# Patterns are tried in order; first match wins.
_AGE_RANGE_PATTERNS: list[tuple[re.Pattern, object]] = [
    # "moins de N mois" / "< N mois" → all sub-year: (0, 0)
    (re.compile(r"(?:moins\s*de|<\s*)\s*\d+\s*mois", re.IGNORECASE), lambda m: (0, 0)),
    # "<= N ans"
    (re.compile(r"<=\s*(\d+)\s*ans?", re.IGNORECASE), lambda m: (0, min(int(m.group(1)), 17))),
    # "< N ans" / "moins de N ans" / "âgé de moins de N ans"
    (re.compile(r"(?:moins\s*de|<)\s*(\d+)\s*ans?", re.IGNORECASE), lambda m: (0, min(int(m.group(1)) - 1, 17))),
    # ">= N ans"
    (re.compile(r">=\s*(\d+)\s*ans?", re.IGNORECASE), lambda m: (int(m.group(1)), 17)),
    # "à partir de N ans" / "plus de N ans" / "N ans et plus"
    (re.compile(r"(?:à\s*partir\s*de|plus\s*de)\s*(\d+)\s*ans?", re.IGNORECASE), lambda m: (int(m.group(1)), 17)),
    (re.compile(r"(\d+)\s*ans?\s*et\s*plus", re.IGNORECASE), lambda m: (int(m.group(1)), 17)),
    # "de N à M ans" / "N à M ans" / "N-M ans" (digits on both sides, followed by "ans")
    (re.compile(r"(\d+)\s*[-à]\s*(\d+)\s*ans?", re.IGNORECASE), lambda m: (int(m.group(1)), int(m.group(2)))),
]


def parse_explicit_age_range(text: str) -> tuple[int, int] | None:
    """Extract an explicit age range from a text block.

    Returns (min_year, max_year) in integer years, or None if no range is found.
    Age 0 means neonate/newborn; all sub-year ages map to year 0.
    """
    for pattern, handler in _AGE_RANGE_PATTERNS:
        m = pattern.search(text)
        if m:
            return handler(m)
    return None


def text_block_covers_age(text: str, age: int) -> bool:
    """Return True if the text block is relevant for the given age (in years).

    Tries explicit age ranges first; falls back to keyword implicit ranges.
    """
    explicit = parse_explicit_age_range(text)
    if explicit is not None:
        return explicit[0] <= age <= explicit[1]

    text_lower = text.lower()
    for kw in pediatric_config.PEDIATRIC_KEYWORDS:
        if kw in text_lower:
            lo, hi = pediatric_config.KEYWORD_AGE_RANGES[kw]
            if lo <= age <= hi:
                return True
    return False


def classify(rcp_json: dict, atc_code: str = "", age: int | None = None) -> PediatricClassification:
    """Classify a drug for pediatric use based on its parsed RCP.

    Args:
        rcp_json: Parsed RCP JSON (with "source" and "content" keys).
        atc_code: ATC code of the drug (e.g. "G03AA07").

    Returns:
        PediatricClassification with conditions A, B, C and matched evidence.
    """
    source = rcp_json.get("source", {})
    cis = source.get("cis", "") if isinstance(source, dict) else ""
    result = PediatricClassification(cis=cis)

    # --- Sections 4.1 + 4.2: Indication / Sur avis ---
    texts_41 = extract_section_texts(rcp_json, "4.1")
    texts_42 = extract_section_texts(rcp_json, "4.2")
    texts_41_42 = texts_41 + texts_42

    has_any_keyword = False
    has_positive = False
    has_negative = False
    has_keyword_no_indication = False

    for text in texts_41_42:
        if age is not None and not text_block_covers_age(text, age):
            continue
        keywords = find_pediatric_keywords_in_text(text)
        if not keywords:
            continue

        has_any_keyword = True
        neg = matches_negative_pattern(text)

        if neg:
            has_negative = True
            result.matches_41_42.append(SentenceMatch(text=text, keywords=keywords, negative_pattern=neg))
        elif pediatric_config.REQUIRE_POSITIVE_INDICATION:
            # Strict mode: need an explicit indication phrase
            if matches_positive_indication(text):
                has_positive = True
                result.matches_41_42.append(SentenceMatch(text=text, keywords=keywords, is_positive=True))
            else:
                has_keyword_no_indication = True
        else:
            # Permissive mode: keyword without negative → positive
            has_positive = True
            result.matches_41_42.append(SentenceMatch(text=text, keywords=keywords, is_positive=True))

    # "réservé à l'adulte" check on full 4.1/4.2 text
    full_text_41_42 = " ".join(texts_41_42)
    adult_reserved = is_adult_reserved(full_text_41_42)

    # Contraceptive ATC check
    is_contraceptive = bool(atc_code and atc_code.upper().startswith("G03"))

    # A: Indication pédiatrique
    if has_positive:
        result.a_reasons.append("keyword positif en 4.1/4.2")
    result.condition_a = has_positive

    # C: Sur avis d'un professionnel de santé
    if has_negative:
        result.c_reasons.append("phrases négatives en 4.1/4.2")
    if has_keyword_no_indication and not has_positive:
        result.c_reasons.append("keyword sans indication explicite en 4.1/4.2")
    if not has_any_keyword:
        result.c_reasons.append("pas de mention pédiatrique en 4.1/4.2")
    if adult_reserved:
        result.c_reasons.append("réservé à l'adulte")
    if is_contraceptive:
        result.c_reasons.append("contraceptif (ATC G03)")
    result.condition_c = len(result.c_reasons) > 0

    # --- Section 4.3: Contre-indications ---
    texts_43 = extract_section_texts(rcp_json, "4.3")
    for text in texts_43:
        if age is not None and not text_block_covers_age(text, age):
            continue
        keywords = find_pediatric_keywords_in_text(text)
        if keywords:
            result.matches_43.append(SentenceMatch(text=text, keywords=keywords))

    if result.matches_43:
        result.b_reasons.append("mention pédiatrique en 4.3")
    result.condition_b = len(result.matches_43) > 0

    if tbp := pediatric_config.TIE_BREAKER_PRIORITY:
        prediction = ""
        prediction += "A" if result.condition_a else ""
        prediction += "B" if result.condition_b else ""
        prediction += "C" if result.condition_c else ""
        if len(prediction) >= 2:
            override = tbp[prediction]
            result.condition_a = "A" in override
            result.condition_b = "B" in override
            result.condition_c = "C" in override

    return result


# --- Evaluation ---


def load_ground_truth(path: str) -> dict[str, dict]:
    """Load ground truth CSV. Returns dict keyed by CIS code.

    Expected format: CIS,A:...,B:...,C:... (4 columns, oui/non values).
    """
    gt = {}
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames or []
        cis_col, a_col, b_col, c_col = cols[0], cols[1], cols[2], cols[3]

        for row in reader:
            cis = row[cis_col].strip()
            gt[cis] = {
                "A": row[a_col].strip().lower() == "oui",
                "B": row[b_col].strip().lower() == "oui",
                "C": row[c_col].strip().lower() == "oui",
            }
    return gt


def compute_metrics(
    predictions: list[PediatricClassification],
    ground_truth: dict[str, dict],
) -> dict:
    """Compute classification metrics.

    Returns dict with per-label and overall metrics.
    """
    labels = ["A", "B", "C"]
    metrics: dict = {}
    total_correct_all = 0
    total_evaluated = 0

    for label in labels:
        tp = fp = fn = tn = 0
        for pred in predictions:
            if pred.cis not in ground_truth:
                continue
            gt = ground_truth[pred.cis]
            pred_val = {"A": pred.condition_a, "B": pred.condition_b, "C": pred.condition_c}[label]
            truth_val = gt[label]

            if pred_val and truth_val:
                tp += 1
            elif pred_val and not truth_val:
                fp += 1
            elif not pred_val and truth_val:
                fn += 1
            else:
                tn += 1

        total = tp + fp + fn + tn
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        accuracy = (tp + tn) / total if total > 0 else 0.0

        metrics[label] = {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "accuracy": accuracy,
        }

    # Overall: all 3 labels correct
    for pred in predictions:
        if pred.cis not in ground_truth:
            continue
        total_evaluated += 1
        gt = ground_truth[pred.cis]
        if pred.condition_a == gt["A"] and pred.condition_b == gt["B"] and pred.condition_c == gt["C"]:
            total_correct_all += 1

    metrics["overall"] = {
        "evaluated": total_evaluated,
        "exact_match": total_correct_all,
        "exact_match_rate": total_correct_all / total_evaluated if total_evaluated > 0 else 0.0,
    }

    return metrics


def format_metrics(metrics: dict) -> str:
    """Format metrics as a human-readable report."""
    lines = []
    lines.append("=" * 60)
    lines.append("PEDIATRIC CLASSIFICATION METRICS")
    lines.append("=" * 60)

    for label, name in [("A", "Indication"), ("B", "Contre-indication"), ("C", "Sur avis")]:
        m = metrics[label]
        lines.append(f"\n{label}: {name}")
        lines.append(f"  Accuracy:  {m['accuracy']:.1%}")
        lines.append(f"  Precision: {m['precision']:.1%}")
        lines.append(f"  Recall:    {m['recall']:.1%}")
        lines.append(f"  F1:        {m['f1']:.1%}")
        lines.append(f"  (TP={m['tp']} FP={m['fp']} FN={m['fn']} TN={m['tn']})")

    o = metrics["overall"]
    lines.append(f"\nOverall exact match: {o['exact_match']}/{o['evaluated']} ({o['exact_match_rate']:.1%})")
    lines.append("=" * 60)
    return "\n".join(lines)
