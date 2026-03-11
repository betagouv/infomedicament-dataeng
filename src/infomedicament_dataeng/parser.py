"""HTML parsing logic for medication documents."""

import re

from bs4 import BeautifulSoup


def normaliser_texte(texte: str) -> str:
    """Normalize Unicode characters in extracted text.

    Replaces curly quotes, non-breaking hyphens, and en-dashes so downstream
    regex matching works consistently.
    """
    return (
        texte
        .replace("\u2019", "'")   # right single quotation mark
        .replace("\u2018", "'")   # left single quotation mark
        .replace("\u2011", "-")   # non-breaking hyphen
        .replace("\u2013", "-")   # en dash
        .replace("\u2265", ">=")  # change ≥ to >=
        .replace("\u2264", "<=")  # change ≤ to <=
    )


def traiter_images_dans_html(contenu_html):
    """
    Process img tags in HTML content by converting relative URLs to absolute URLs
    and adding W3C-compliant self-closing tags.
    """
    if not contenu_html or "<img" not in contenu_html:
        return contenu_html

    # Pattern to match img tags with relative src
    img_pattern = r'<img([^>]*?)src="\.\.\/images\/([^"]+)"([^>]*?)(?:\s*\/)?>'

    def replace_img(match):
        before_src = match.group(1)
        image_path = match.group(2)
        after_src = match.group(3)

        # Build absolute URL
        absolute_url = f"https://cellar-c2.services.clever-cloud.com/info-medicaments/exports/images/{image_path}"

        # Rebuild tag with absolute URL and W3C self-closing
        return f'<img{before_src}src="{absolute_url}"{after_src} />'

    return re.sub(img_pattern, replace_img, contenu_html, flags=re.IGNORECASE)


def nettoyer_element_pour_texte(element):
    """
    Clean an element by removing letter-spacing spans while preserving spaces
    and converting sup/sub tags to Unicode characters.
    """
    # Conversion dictionaries for superscripts and subscripts
    exposants = {
        "0": "⁰",
        "1": "¹",
        "2": "²",
        "3": "³",
        "4": "⁴",
        "5": "⁵",
        "6": "⁶",
        "7": "⁷",
        "8": "⁸",
        "9": "⁹",
        "a": "ᵃ",
        "b": "ᵇ",
        "c": "ᶜ",
        "d": "ᵈ",
        "e": "ᵉ",
        "f": "ᶠ",
        "g": "ᵍ",
        "h": "ʰ",
        "i": "ⁱ",
        "j": "ʲ",
        "k": "ᵏ",
        "l": "ˡ",
        "m": "ᵐ",
        "n": "ⁿ",
        "o": "ᵒ",
        "p": "ᵖ",
        "r": "ʳ",
        "s": "ˢ",
        "t": "ᵗ",
        "u": "ᵘ",
        "v": "ᵛ",
        "w": "ʷ",
        "x": "ˣ",
        "y": "ʸ",
        "z": "ᶻ",
        "A": "ᴬ",
        "B": "ᴮ",
        "D": "ᴰ",
        "E": "ᴱ",
        "G": "ᴳ",
        "H": "ᴴ",
        "I": "ᴵ",
        "J": "ᴶ",
        "K": "ᴷ",
        "L": "ᴸ",
        "M": "ᴹ",
        "N": "ᴺ",
        "O": "ᴼ",
        "P": "ᴾ",
        "R": "ᴿ",
        "T": "ᵀ",
        "U": "ᵁ",
        "V": "ⱽ",
        "W": "ᵂ",
        "+": "⁺",
        "-": "⁻",
        "=": "⁼",
        "(": "⁽",
        ")": "⁾",
    }

    indices = {
        "0": "₀",
        "1": "₁",
        "2": "₂",
        "3": "₃",
        "4": "₄",
        "5": "₅",
        "6": "₆",
        "7": "₇",
        "8": "₈",
        "9": "₉",
        "a": "ₐ",
        "e": "ₑ",
        "h": "ₕ",
        "i": "ᵢ",
        "j": "ⱼ",
        "k": "ₖ",
        "l": "ₗ",
        "m": "ₘ",
        "n": "ₙ",
        "o": "ₒ",
        "p": "ₚ",
        "r": "ᵣ",
        "s": "ₛ",
        "t": "ₜ",
        "u": "ᵤ",
        "v": "ᵥ",
        "x": "ₓ",
        "+": "₊",
        "-": "₋",
        "=": "₌",
        "(": "₍",
        ")": "₎",
    }

    def convertir_exposant(texte):
        """Convert text to Unicode superscript."""
        resultat = ""
        for char in texte:
            if char in exposants:
                resultat += exposants[char]
            elif char == " ":
                resultat += char  # Preserve spaces
            else:
                # For unsupported characters, keep as-is
                resultat += char
        return resultat

    def convertir_indice(texte):
        """Convert text to Unicode subscript."""
        resultat = ""
        for char in texte:
            if char in indices:
                resultat += indices[char]
            elif char == " ":
                resultat += char  # Preserve spaces
            else:
                # For unsupported characters, keep as-is
                resultat += char
        return resultat

    # Create a copy of the element to avoid modifying the original
    element_copy = element.__copy__()

    # First, process sup and sub tags
    for sup in element_copy.find_all("sup"):
        texte_sup = sup.get_text()
        texte_converti = convertir_exposant(texte_sup)
        sup.replace_with(texte_converti)

    for sub in element_copy.find_all("sub"):
        texte_sub = sub.get_text()
        texte_converti = convertir_indice(texte_sub)
        sub.replace_with(texte_converti)

    # Then, process spans with letter-spacing
    spans_letter_spacing = element_copy.find_all("span", style=lambda x: x and "letter-spacing" in x)

    for span in spans_letter_spacing:
        # Replace span with its text content (which may be a space)
        span.replace_with(span.get_text())

    return element_copy


def extraire_styles(element):
    """Extract styles from an HTML element (without sup/sub as they are converted to Unicode)."""
    styles = set()
    for span in element.find_all(["span", "b", "strong", "u", "em"]):
        cls = span.get("class", [])
        for c in cls:
            if c == "gras":
                styles.add("bold")
            if c == "souligne":
                styles.add("underline")
        if span.name in ("b", "strong"):
            styles.add("bold")
        if span.name == "u":
            styles.add("underline")
        if span.name == "em":
            styles.add("italic")
    return sorted(styles)


def extraire_attributs_html(element):
    """Extract all HTML attributes from an element."""
    attributs = {}
    for attr, valeur in element.attrs.items():
        if isinstance(valeur, list):
            attributs[attr] = " ".join(valeur)
        else:
            attributs[attr] = valeur
    return attributs


def extraire_contenu_cellule(cellule):
    """Extract cell content WITHOUT duplicating the main text."""
    # Process images before extracting content
    cellule_html = str(cellule)
    cellule_html_traite = traiter_images_dans_html(cellule_html)

    # Recreate element with processed images
    if cellule_html_traite != cellule_html:
        cellule = BeautifulSoup(cellule_html_traite, "html.parser").find(cellule.name)

    contenu = {
        "tag": cellule.name,
        "attributes": extraire_attributs_html(cellule),
        "text": normaliser_texte(nettoyer_element_pour_texte(cellule).get_text()),
        "html": cellule_html_traite,  # Add complete HTML with images
        "children": [],
    }

    # Only extract children if they provide different structural information
    # and not just a repetition of the same text
    cellule_text_nettoye = nettoyer_element_pour_texte(cellule).get_text()

    for enfant in cellule.children:
        if hasattr(enfant, "name") and enfant.name:
            if enfant.name in ["p", "div", "span"]:
                enfant_text = normaliser_texte(nettoyer_element_pour_texte(enfant).get_text())
                # ONLY add child if its text differs from parent
                # or if it has specific styles
                enfant_styles = extraire_styles(enfant)
                if enfant_text != cellule_text_nettoye or enfant_styles:
                    enfant_html = str(enfant)
                    enfant_html_traite = traiter_images_dans_html(enfant_html)

                    enfant_data = {
                        "tag": enfant.name,
                        "attributes": extraire_attributs_html(enfant),
                        "text": enfant_text,
                        "html": enfant_html_traite,
                        "styles": enfant_styles,
                    }
                    contenu["children"].append(enfant_data)

    return contenu


def extraire_table_complete(table_element):
    """Extract a complete table with its hierarchical structure."""
    # Process images in the table
    table_html = str(table_element)
    table_html_traite = traiter_images_dans_html(table_html)

    # Recreate table element with processed images
    if table_html_traite != table_html:
        table_element = BeautifulSoup(table_html_traite, "html.parser").find("table")

    table_data = {
        "type": "table",
        "tag": "table",
        "attributes": extraire_attributs_html(table_element),
        "html": table_html_traite,  # Add complete HTML with images
        "children": [],
    }

    # Process thead if present
    thead = table_element.find("thead")
    if thead:
        thead_data = {"tag": "thead", "attributes": extraire_attributs_html(thead), "children": []}

        for tr in thead.find_all("tr", recursive=False):
            tr_data = {"tag": "tr", "attributes": extraire_attributs_html(tr), "children": []}

            for cellule in tr.find_all(["th", "td"]):
                cellule_data = extraire_contenu_cellule(cellule)
                tr_data["children"].append(cellule_data)

            thead_data["children"].append(tr_data)

        table_data["children"].append(thead_data)

    # Process tbody if present, otherwise process tr directly
    tbody = table_element.find("tbody")
    if tbody:
        tbody_data = {"tag": "tbody", "attributes": extraire_attributs_html(tbody), "children": []}

        for tr in tbody.find_all("tr", recursive=False):
            tr_data = {"tag": "tr", "attributes": extraire_attributs_html(tr), "children": []}

            for cellule in tr.find_all(["th", "td"]):
                cellule_data = extraire_contenu_cellule(cellule)
                tr_data["children"].append(cellule_data)

            tbody_data["children"].append(tr_data)

        table_data["children"].append(tbody_data)
    else:
        # Process tr that are not in thead
        for tr in table_element.find_all("tr", recursive=False):
            # Skip if already processed in thead
            if thead and tr.parent == thead:
                continue

            tr_data = {"tag": "tr", "attributes": extraire_attributs_html(tr), "children": []}

            for cellule in tr.find_all(["th", "td"]):
                cellule_data = extraire_contenu_cellule(cellule)
                tr_data["children"].append(cellule_data)

            table_data["children"].append(tr_data)

    # Process tfoot if present
    tfoot = table_element.find("tfoot")
    if tfoot:
        tfoot_data = {"tag": "tfoot", "attributes": extraire_attributs_html(tfoot), "children": []}

        for tr in tfoot.find_all("tr", recursive=False):
            tr_data = {"tag": "tr", "attributes": extraire_attributs_html(tr), "children": []}

            for cellule in tr.find_all(["th", "td"]):
                cellule_data = extraire_contenu_cellule(cellule)
                tr_data["children"].append(cellule_data)

            tfoot_data["children"].append(tr_data)

        table_data["children"].append(tfoot_data)

    return table_data


def html_vers_json(contenu_html):
    """Convert HTML content to JSON structure WITHOUT structural duplicates."""
    soup = BeautifulSoup(contenu_html, "html.parser")
    resultats = []
    current_titre1 = None
    current_titre2 = None

    # Set to track already processed elements (avoid duplicates)
    elements_traites = set()

    def ajouter_noeud(noeud):
        if current_titre2:
            current_titre2["children"].append(noeud)
        elif current_titre1:
            current_titre1["children"].append(noeud)
        else:
            resultats.append(noeud)

    def est_dans_table(element):
        """Check if an element is inside a table."""
        parent = element.parent
        while parent:
            if parent.name == "table":
                return True
            parent = parent.parent
        return False

    def marquer_descendants_comme_traites(element):
        """Mark all descendants of an element as processed."""
        for descendant in element.find_all():
            if hasattr(descendant, "attrs") and descendant.get("class"):
                elements_traites.add(id(descendant))

    elements = list(soup.find_all(class_=True))
    i = 0
    while i < len(elements):
        element = elements[i]

        # Check if this element has already been processed
        if id(element) in elements_traites:
            i += 1
            continue

        classes = element.get("class")
        if not classes:
            i += 1
            continue

        classe = classes[0]
        # Use cleaned version to extract text
        texte = normaliser_texte(nettoyer_element_pour_texte(element).get_text())

        ancre = None
        a_tag = element.find("a")
        if a_tag and a_tag.has_attr("name"):
            ancre = a_tag["name"]

        if classe in ("AmmAnnexeTitre1", "AmmNoticeTitre1"):
            current_titre1 = {"type": classe, "content": texte, "anchor": ancre if ancre else None, "children": []}
            resultats.append(current_titre1)
            current_titre2 = None
            elements_traites.add(id(element))
            i += 1
            continue

        elif classe == "AmmAnnexeTitre2":
            if current_titre1 is None:
                i += 1
                continue
            current_titre2 = {"type": classe, "content": texte, "anchor": ancre if ancre else None, "children": []}
            current_titre1["children"].append(current_titre2)
            elements_traites.add(id(element))
            i += 1
            continue

        elif classe in ("AmmListePuces1", "AmmListePuces2", "AmmListePuces3"):
            puces = []
            while i < len(elements) and elements[i].get("class", [None])[0] == classe:
                # Check that element is not inside a table
                if not est_dans_table(elements[i]):
                    puce_texte = normaliser_texte(nettoyer_element_pour_texte(elements[i]).get_text())
                    if puce_texte:
                        puces.append(puce_texte)
                    elements_traites.add(id(elements[i]))
                i += 1
            if puces:
                ajouter_noeud({"type": "listePuce", "content": puces})
            continue

        elif classe == "AmmCorpsTexteTable" and element.name == "table":
            # Complete table processing
            table_data = extraire_table_complete(element)
            ajouter_noeud(table_data)
            # Mark ALL descendants of the table as processed
            marquer_descendants_comme_traites(element)
            elements_traites.add(id(element))
            i += 1
            continue

        elif classe in ("AmmCorpsTexte", "AmmCorpsTexteGras"):
            # IMPORTANT: Check that this element is not inside a table
            if est_dans_table(element):
                elements_traites.add(id(element))
                i += 1
                continue

            # Process images in content
            element_html = str(element)
            element_html_traite = traiter_images_dans_html(element_html)

            styles = extraire_styles(element)
            if classe == "AmmCorpsTexteGras" and "bold" not in styles:
                styles.append("bold")
            noeud = {
                "type": classe,
                "content": texte,
                "html": element_html_traite,  # Add HTML with processed images
                "styles": sorted(styles),
            }
            if ancre:
                noeud["anchor"] = ancre
            ajouter_noeud(noeud)
            elements_traites.add(id(element))
            i += 1
            continue

        else:
            # For other types, also check they are not inside a table
            if not est_dans_table(element):
                # Process images for other types too
                element_html = str(element)
                element_html_traite = traiter_images_dans_html(element_html)

                noeud = {"type": classe, "content": texte, "html": element_html_traite}
                if ancre:
                    noeud["anchor"] = ancre
                ajouter_noeud(noeud)
            elements_traites.add(id(element))
            i += 1

    return resultats
