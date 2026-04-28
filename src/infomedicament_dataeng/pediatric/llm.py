"""LLM-based pediatric classification using Albert chat completions API."""

import json
import logging

import openai
from openai import OpenAI
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .pediatric import PediatricClassification, classify, extract_section_texts

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
Tu es un expert en pharmacologie pédiatrique. On te donne le contenu des sections 4.1 \
(Indications), 4.2 (Posologie) et 4.3 (Contre-indications) d'un RCP (Résumé des \
Caractéristiques du Produit) de médicament.

Tu dois classer ce médicament selon ces trois conditions (non exclusives) :
- A (Indication pédiatrique) : le médicament a une indication explicite chez l'enfant ou le \
nourrisson (usage normal ou recommandé chez les moins de 18 ans)
- B (Contre-indication pédiatrique) : le médicament est explicitement contre-indiqué \
chez l'enfant ou le nourrisson et cette contre-indication se trouve spécifiquement dans la section 4.3
- C (Sur avis d'un professionnel de santé) : il n'y a pas d'indication claire, les \
données sont insuffisantes, ou l'usage pédiatrique nécessite une surveillance particulière.

Si tu repères une contre-indication pédiatrique explicite mais qui n'est pas dans la section 4.3, \
tu ne dois pas cocher la condition B (contre-indication pédiatrique) mais tu peux cocher la condition C.

Réponds uniquement avec un objet JSON valide, sans texte avant ou après :
{"A": true/false, "B": true/false, "C": true/false, "reasoning": "explication courte en français"}"""


@retry(
    retry=retry_if_exception_type((openai.RateLimitError, openai.APIStatusError)),
    wait=wait_exponential(multiplier=1, min=1, max=16),
    stop=stop_after_attempt(4),
    reraise=True,
)
def _call_completion(client: OpenAI, model: str, user_content: str) -> str:
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        temperature=0,
    )
    return response.choices[0].message.content or ""


def classify_with_llm(
    rcp_json: dict,
    atc_code: str,
    client: OpenAI,
    model: str,
) -> PediatricClassification:
    source = rcp_json.get("source", {})
    cis = source.get("cis", "") if isinstance(source, dict) else ""

    texts_41 = extract_section_texts(rcp_json, "4.1")
    texts_42 = extract_section_texts(rcp_json, "4.2")
    texts_43 = extract_section_texts(rcp_json, "4.3")

    user_content = (
        f"## Section 4.1 – Indications\n{'\n'.join(texts_41) or '(vide)'}\n\n"
        f"## Section 4.2 – Posologie\n{'\n'.join(texts_42) or '(vide)'}\n\n"
        f"## Section 4.3 – Contre-indications\n{'\n'.join(texts_43) or '(vide)'}"
    )

    try:
        raw = _call_completion(client, model, user_content).strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        parsed = json.loads(raw)

        result = PediatricClassification(cis=cis)
        result.condition_a = bool(parsed.get("A", False))
        result.condition_b = bool(parsed.get("B", False))
        result.condition_c = bool(parsed.get("C", False))
        reasoning = parsed.get("reasoning", "")
        if result.condition_a:
            result.a_reasons = [reasoning]
        if result.condition_b:
            result.b_reasons = [reasoning]
        if result.condition_c:
            result.c_reasons = [reasoning]
        return result

    except Exception as e:
        logger.warning(f"LLM classification failed for CIS {cis} ({e}), falling back to keyword classifier")
        return classify(rcp_json, atc_code)
