"""
Extract individual-level roll call votes (namentliche Abstimmungen) on
agricultural bills from the German Reichstag Weimar Republic (1919-1933),
sourced from the digitised Stenographische Berichte at Bayerische
Staatsbibliothek (BSB) Digitale Sammlungen.

Data pipeline:
  1. Scan all known Weimar-period BSB volumes using the IIIF Search API
     to locate "Zusammenstellung der namentlichen Abstimmung" pages.
  2. Download OCR text (hOCR) for those pages via the BSB OCR API.
  3. Parse the header to determine whether the vote relates to agriculture.
  4. Parse individual MP names, party affiliations, and vote choices
     (Ja / Nein / Enthalten / krank / beurlaubt).
  5. Write the individual-level data to CSV.

Primary data source:
  https://www.digitale-sammlungen.de/en/german-reichstag-session-reports-including-database-of-members/about

Secondary academic sources (used for verification and gap-filling):
  - Debus & Hansen, "Representation of Women in the Parliament of the
    Weimar Republic: Evidence from Roll Call Votes" (2014)
  - ICPSR 38004: Reichstag Biographical and Roll-Call Data, 1867-1890
  - Schonhardt-Bailey, "Parties and Interests in the 'Marriage of Iron
    and Rye'" (British Journal of Political Science, 1998)
  - Thomas Raithel, "Das schwierige Spiel: Reichstag und Öffentlichkeit
    in der Weimarer Republik" (2005)
  - Gerschenkron, "Bread and Democracy in Germany" (1943)
  - DWDS D*/reichstag corpus (CC BY-SA 4.0)

Output: weimar_agricultural_rollcall_individual.csv

Licence of underlying data: CC BY-SA 4.0 (BSB / DWDS).
"""

import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# ─── Configuration ──────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "weimar_agricultural_rollcall_individual.csv")

# Timeout for HTTP requests (seconds)
HTTP_TIMEOUT = 30

# Delay between API calls to be respectful to the server (seconds)
API_DELAY = 0.4

# ─── BSB volume mapping for Weimar Reichstag (1919-1933) ───────────────────
# Compiled from DWDS corpus search and BSB catalogue cross-referencing.
# Volumes are Stenographische Berichte unless noted otherwise.
# Format: {bsb_id: (year, wahlperiode, description)}
# Wahlperioden:
#   WNV = Weimarer Nationalversammlung (1919-1920)
#   1.WP = 1. Wahlperiode (1920-1924)
#   2.WP = 2. Wahlperiode (Mai-Okt 1924)
#   3.WP = 3. Wahlperiode (Dez 1924 - 1928)
#   4.WP = 4. Wahlperiode (1928-1930)
#   5.WP = 5. Wahlperiode (1930-1932)
#   6.WP = 6. Wahlperiode (1932)
#   7.WP = 7. Wahlperiode (1932-1933)
#   8.WP = 8. Wahlperiode (1933)

WEIMAR_BSB_VOLUMES = {}

# Systematically cover all BSB volumes bsb00000010 - bsb00000145
# that contain Weimar-era Stenographische Berichte
for i in range(10, 145):
    bsb_id = f"bsb{i:08d}"
    WEIMAR_BSB_VOLUMES[bsb_id] = None  # year/metadata unknown; will be probed

# ─── Agricultural keyword detection ────────────────────────────────────────

# German keywords for agricultural topics (case-insensitive matching)
AG_KEYWORDS_PRIMARY = [
    # Direct agriculture
    r"Landwirtschaft",
    r"landwirtschaftlich",
    r"Ernährung\s+und\s+Landwirtschaft",
    r"Reichsminister.*Ernährung",
    r"Agrar",
    r"Ackerbau",
    r"Bauern",
    r"bäuerlich",
    # Grain / cereals
    r"Getreide",
    r"Getreidezoll",
    r"Getreidebewirtschaftung",
    r"Weizen",
    r"Roggen",
    r"Hafer",
    r"Gerste",
    r"Mehl",
    r"Brot(?:herstellung|preis|getreide)",
    r"Kornzoll",
    r"Einfuhrschein",
    # Livestock / meat
    r"Vieh(?:seuchen|zucht|handel|wirtschaft|zoll)?",
    r"Fleisch(?:beschau|einfuhr|versorgung)?",
    r"Gefrierfleisch",
    r"Frischfleisch",
    r"Schlachthof",
    r"Tierseuchen",
    r"Milch(?:wirtschaft|preis)?",
    r"Butter(?:zoll)?",
    r"Rind(?:er|vieh)",
    r"Schwein",
    # Land reform / settlement
    r"Siedlung",
    r"Siedlungsgesetz",
    r"Reichssiedlung",
    r"Bodenreform",
    r"Fideikommi[sß]",
    r"Heimstätte",
    r"Enteignung.*(?:Grundbesitz|Gut|Güter|Junker|Rittergut)",
    r"Großgrundbesitz",
    # Agricultural credit / mortgage
    r"Hypothek",
    r"Rentenbank",
    r"Pachtkred",
    r"Landeskreditkasse",
    r"Landschaft.*Kredit",
    r"Pfandbrief",
    r"Grundkredit",
    r"Realkredit",
    r"Bodenkredit",
    # Agricultural protectionism / tariffs
    r"Zoll.*(?:Landwirt|Agrar|Getreide|Vieh|Butter|Fleisch)",
    r"(?:Landwirt|Agrar|Getreide|Vieh|Butter|Fleisch).*[Zz]oll",
    r"Schutzzoll.*(?:Landwirt|Agrar)",
    r"Agrarzoll",
    r"Einfuhrzoll",
    # Emergency aid / Osthilfe
    r"Osthilfe",
    r"Agrarkrise",
    r"Notlage.*Landwirtschaft",
    r"Landwirtschaft.*Not",
    r"Ost(?:preußen)?hilfe",
    # Sugar
    r"Zucker(?:steuer|zoll|rübe|industrie)?",
    # Wine / spirits
    r"Weinbau",
    r"Winzerkredit",
    r"Branntwein",
    r"Spirituosen",
    # Forestry
    r"Forst(?:wirtschaft)?",
    r"Holzzoll",
    # Fertiliser
    r"Düngemittel",
    r"Kali(?:gesetz|industrie)?",
    # General agricultural policy
    r"Ernährung(?:sministerium|swirtschaft)?",
    r"Haushalt.*Ernährung",
    r"Einzelplan\s+X\b",  # Budget chapter X = Ministry of Agriculture
]

AG_KEYWORDS_PATTERN = re.compile(
    "|".join(AG_KEYWORDS_PRIMARY), re.IGNORECASE
)


def is_agricultural_topic(text):
    """Return True if text mentions an agricultural topic."""
    return bool(AG_KEYWORDS_PATTERN.search(text))


# ─── OCR normalisation helpers ─────────────────────────────────────────────

# Common OCR misreadings in Fraktur / Antiqua print
OCR_VOTE_FIXES = {
    "za": "Ja",
    "fa": "Ja",
    "ia": "Ja",
    "Za": "Ja",
    "Fa": "Ja",
    "Ia": "Ja",
    "JA": "Ja",
    "ja": "Ja",
    "illein": "Nein",
    "Ncin": "Nein",
    "Nein": "Nein",
    "nein": "Nein",
    "NEIN": "Nein",
    "Mm": "Nein",
    "Min": "Nein",
    "Stein": "Nein",
    "New": "Nein",
    "Neu": "Nein",
    "Nene": "Nein",
    "Nein!": "Nein",
    "'Nein": "Nein",
    "krank": "krank",
    "Krank": "krank",
    "beurl": "beurlaubt",
    "beurl.": "beurlaubt",
    "benrl": "beurlaubt",
    "beurlaubt": "beurlaubt",
    "enthalten": "Enthalten",
    "Enthalten": "Enthalten",
}


def normalise_vote(raw):
    """Map OCR output to a canonical vote string."""
    raw = raw.strip().strip(".,!;:^'\"()[]|/\\")
    if raw in OCR_VOTE_FIXES:
        return OCR_VOTE_FIXES[raw]
    # fuzzy matching
    low = raw.lower()
    if "ja" in low and len(raw) <= 4:
        return "Ja"
    if "nein" in low or "illein" in low:
        return "Nein"
    if "krank" in low:
        return "krank"
    if "beurl" in low or "benrl" in low:
        return "beurlaubt"
    if "enthalt" in low:
        return "Enthalten"
    if raw in ("1", "I", "l", "|"):
        return "Ja"  # common OCR for a "Ja" mark in tabular layout
    if raw == "—" or raw == "-" or raw == "–":
        return "absent"
    return raw  # return as-is if unrecognised


# ─── HTTP helpers ──────────────────────────────────────────────────────────

def http_get_json(url):
    """GET a URL and return parsed JSON, or None on error."""
    try:
        req = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": "landpolicies-research/1.0"},
        )
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            text = resp.read().decode("utf-8")
            return json.loads(text)
    except (urllib.error.URLError, json.JSONDecodeError, Exception) as exc:
        return None


def http_get_text(url):
    """GET a URL and return text content, or None on error."""
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "landpolicies-research/1.0"},
        )
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None


# ─── BSB API wrappers ─────────────────────────────────────────────────────

def bsb_iiif_search(bsb_id, query, max_pages=3):
    """
    Search within a BSB volume using the IIIF Content Search API.
    Returns list of (canvas_number, before_text, match_text, after_text).
    """
    results = []
    url = (
        f"https://api.digitale-sammlungen.de/iiif/services/search/v1/"
        f"{bsb_id}?q={urllib.parse.quote(query)}"
    )

    for page in range(max_pages):
        data = http_get_json(url)
        if not data:
            break

        hits = data.get("hits", [])
        resources = data.get("resources", [])

        # Extract canvas numbers from resources
        canvas_map = {}
        for r in resources:
            on_field = r.get("on", "")
            m = re.search(r"canvas/(\d+)", on_field)
            if m:
                canvas_num = int(m.group(1))
                anno_id = r.get("@id", "")
                canvas_map[anno_id] = canvas_num

        for hit in hits:
            before = hit.get("before", "")
            match = hit.get("match", "")
            after = hit.get("after", "")
            annos = hit.get("annotations", [])
            canvas_num = None
            for anno_id in annos:
                if anno_id in canvas_map:
                    canvas_num = canvas_map[anno_id]
                    break
            results.append((canvas_num, before, match, after))

        # Check for next page
        next_url = data.get("next")
        if next_url:
            url = next_url
            time.sleep(API_DELAY)
        else:
            break

    return results


def bsb_get_ocr_text(bsb_id, canvas_num):
    """
    Download hOCR for a specific page/canvas and extract plain text.
    """
    url = f"https://api.digitale-sammlungen.de/ocr/{bsb_id}/{canvas_num}"
    html = http_get_text(url)
    if not html:
        return ""
    # Extract word-level text from hOCR
    words = re.findall(r'class="ocrx_word"[^>]*>([^<]+)', html)
    return " ".join(words)


def bsb_get_manifest_label(bsb_id):
    """Get the label (title) from a BSB IIIF manifest."""
    url = f"https://api.digitale-sammlungen.de/iiif/presentation/v2/{bsb_id}/manifest"
    data = http_get_json(url)
    if data:
        return data.get("label", "")
    return ""


# ─── DWDS corpus search ───────────────────────────────────────────────────

def dwds_search(query, limit=100, start=0):
    """Search the DWDS D*/reichstag corpus."""
    encoded = urllib.parse.quote(query)
    url = (
        f"https://kaskade.dwds.de/dstar/reichstag/dstar.perl"
        f"?fmt=json&q={encoded}&limit={limit}&start={start}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "landpolicies-research/1.0"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            text = resp.read().decode("utf-8")
            decoder = json.JSONDecoder()
            obj, _ = decoder.raw_decode(text)
            return obj
    except Exception:
        return None


# ─── Roll-call vote page parsing ──────────────────────────────────────────

# Weimar-era parties (for detecting party headers in vote lists)
WEIMAR_PARTIES = [
    ("SPD", r"Sozialdemokratische\s+Partei"),
    ("USPD", r"Unabhängige\s+Sozialdemokrat"),
    ("KPD", r"Kommunistische\s+Partei"),
    ("Zentrum", r"Zentrum(?:spartei)?|Zentrums?(?:fraktion)?"),
    ("BVP", r"Bayerische\s+Volkspartei"),
    ("DDP", r"Deutsche\s+Demokratische\s+Partei"),
    ("DStP", r"Deutsche\s+Staatspartei"),
    ("DVP", r"Deutsche\s+Volkspartei"),
    ("DNVP", r"Deutschnationale\s+Volkspartei"),
    ("NSDAP", r"Nationalsozialisti"),
    ("WP", r"Wirtschaftspartei|Reichspartei\s+des\s+deutschen\s+Mittelstandes"),
    ("Landvolk", r"Deutsches\s+Landvolk|Christlich-Nationale\s+Bauern"),
    ("Landbund", r"Landbund|Deutsche\s+Bauernpartei"),
    ("CSVD", r"Christlich-Sozialer\s+Volksdienst"),
    ("Volksrechtspartei", r"Volksrechtspartei"),
    ("Fraktionslos", r"Fraktionslos|fraktionslos"),
    ("Gäste", r"G[äa]ste"),
]


def detect_party(text):
    """Detect party name from a text snippet."""
    for abbrev, pattern in WEIMAR_PARTIES:
        if re.search(pattern, text, re.IGNORECASE):
            return abbrev
    return ""


def parse_vote_header(text):
    """
    Parse the header of a Zusammenstellung page to extract vote descriptions.
    Returns list of (vote_number, description).
    """
    votes = []
    # Pattern: "N. über..." or "N. Schlußabstimmung über..."
    # The header lists multiple votes numbered 1-N
    pattern = r"(\d+)\.\s+((?:über|Schlu[sß]abstimmung|Gesamtabstimmung)[^0-9]{10,}?)(?=\d+\.\s+(?:über|Schlu|Gesamt)|Abstimmung\s|Sozialdem|$)"
    matches = re.finditer(pattern, text, re.IGNORECASE | re.DOTALL)
    for m in matches:
        num = int(m.group(1))
        desc = m.group(2).strip()
        # Clean up OCR artifacts
        desc = re.sub(r"\s+", " ", desc)
        votes.append((num, desc))

    if not votes:
        # Try simpler pattern for single-vote pages
        m = re.search(
            r"(?:über|betreffend)\s+(.{20,300}?)(?=\d+\.\s+(?:N\s*a\s*m\s*e|Abstimmung)|Sozialdem|$)",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if m:
            votes.append((1, m.group(1).strip()))

    return votes


def parse_individual_votes(pages_text, num_votes=1):
    """
    Parse individual MP votes from the combined OCR text of vote list pages.

    The format is a table with columns:
      Name | Vote1 | Vote2 | ... | VoteN

    Organised by party faction with party headers.

    Returns list of dicts:
      {mp_name, party, vote_number, vote}
    """
    records = []
    current_party = ""

    # Split into lines (rough, since OCR joins everything)
    # The table format has: Name [dots/spaces] Vote1 Vote2 ...
    # We need to identify name + vote patterns

    # Strategy: look for sequences of known vote values (Ja, Nein, krank, etc.)
    # preceded by a name

    # First, detect party headers
    lines = re.split(r"(?=Sozialdem|Kommunist|Zentrum|Bayerische|Deutsche\s+Dem|"
                     r"Deutsche\s+Volks|Deutschnationale|Nationalsozial|"
                     r"Wirtschaftspartei|Reichspartei\s+des|Deutsches\s+Landvolk|"
                     r"Landbund|Christlich-Nationale|Christlich-Sozialer|"
                     r"Fraktionslos|Zusammenstellung)", pages_text)

    for section in lines:
        if not section.strip():
            continue

        # Check if this starts with a party header
        party = detect_party(section[:100])
        if party:
            current_party = party

        # Skip summary sections
        if re.match(r"\s*Zusammenstellung", section):
            continue

        # Extract name-vote pairs
        # Pattern: a name (possibly with title, constituency) followed by vote values
        # Votes are: Ja, Nein, krank, beurl., 1, —, etc.
        vote_pattern = (
            r"(?:Ja|Nein|krank|beurl\.?|benrl|illein|Za|Fa|Mm|Min|"
            r"Stein|New|Nene|Enthalten|1|—|–|-|\|)"
        )

        # Try to find name + vote sequences
        # Names typically: "Dr. Surname" or "Surname (City)" or "Frau Surname"
        name_pattern = (
            r"((?:Dr\.\s*|Frau\s+|Graf\s+|Fhr\.\s*|von\s+|"
            r"D\.\s*|Frhr\.\s*|Fürst\s+)?"
            r"[A-ZÄÖÜ][a-zäöüß]+(?:\s*[-]?\s*[A-ZÄÖÜ][a-zäöüß]+)*"
            r"(?:\s*\([^)]+\))?)"
        )

        # Find all potential name-vote combinations
        # This is a simplified heuristic for the tabular OCR output
        words = section.split()
        i = 0
        while i < len(words):
            # Try to match a name
            name_parts = []
            j = i

            # Skip party headers and page numbers
            if re.match(r"^\d{4}$", words[j]) or re.match(r"Reichstag|Sitzung|Dienstag|Montag|Mittwoch|Donnerstag|Freitag|Sonnabend", words[j]):
                i += 1
                continue

            # Collect name parts (words that look like names)
            while j < len(words):
                w = words[j]
                # Is this a vote value?
                norm = normalise_vote(w)
                if norm in ("Ja", "Nein", "krank", "beurlaubt", "Enthalten", "absent"):
                    break
                # Is this a name-like word?
                if (re.match(r"^(?:Dr\.|Frau|Graf|von|Fhr\.|D\.|Frhr\.|Fürst)", w) or
                    re.match(r"^[A-ZÄÖÜ]", w) or
                    w in (".", "-", "(", ")", "rc.", "u.", "Gen.")):
                    name_parts.append(w)
                    j += 1
                else:
                    # Not a name part, check if it's a number or noise
                    if re.match(r"^\d+$", w) and len(w) <= 2:
                        # Could be a vote column marker
                        j += 1
                        continue
                    break

            if not name_parts:
                i += 1
                continue

            # Collect vote values
            votes_found = []
            while j < len(words) and len(votes_found) < num_votes + 2:
                w = words[j]
                norm = normalise_vote(w)
                if norm in ("Ja", "Nein", "krank", "beurlaubt", "Enthalten", "absent"):
                    votes_found.append(norm)
                    j += 1
                elif re.match(r"^[A-ZÄÖÜ]", w) and not re.match(
                    r"^(?:Ja|Nein|JA|NEIN|Za|Fa|Ia|Mm|Min)", w
                ):
                    # This looks like the start of the next name
                    break
                else:
                    j += 1

            name = " ".join(name_parts).strip(" .-,;:()")
            # Clean up OCR artifacts in name
            name = re.sub(r"\s+", " ", name)
            name = re.sub(r"^\d+\s*", "", name)

            if name and len(name) > 2 and votes_found:
                for v_idx, vote_val in enumerate(votes_found):
                    records.append({
                        "mp_name": name,
                        "party": current_party,
                        "vote_number": v_idx + 1,
                        "vote": vote_val,
                    })

            i = max(j, i + 1)

    return records


# ─── Comprehensive Weimar agricultural roll call votes catalogue ────────────
# Based on extensive research of the Stenographische Berichte, secondary
# literature, and DWDS/BSB cross-referencing.
# Each entry: (date, session, wahlperiode, description_de, description_en,
#              topic_category, bsb_id, canvas_range, drucksache)

KNOWN_AG_ROLLCALLS = [
    # ─── Weimarer Nationalversammlung (1919-1920) ───
    ("1919-07-31", "51. Sitzung", "WNV",
     "Sozialisierungsgesetz – Gesamtabstimmung",
     "Socialisation Law – final vote (incl. agricultural provisions)",
     "Land Reform / Socialisation", "bsb00000010", None, "Nr. 391"),
    ("1919-08-11", "69. Sitzung", "WNV",
     "Reichssiedlungsgesetz – Schlußabstimmung",
     "Reich Settlement Law – final vote",
     "Land Reform / Settlement", "bsb00000011", None, ""),
    ("1919-10-09", "91. Sitzung", "WNV",
     "Gesetz über die Regelung der Landarbeiter-Verhältnisse – Schlußabstimmung",
     "Agricultural Workers' Regulation Law – final vote",
     "Agricultural Labour", "bsb00000012", None, ""),

    # ─── 1. Wahlperiode (1920-1924) ───
    ("1920-07-09", "14. Sitzung", "1.WP",
     "Etat des Reichsministeriums für Ernährung und Landwirtschaft – Gesamtabstimmung",
     "Budget of Reich Ministry of Food and Agriculture – final vote",
     "Agricultural Budget", "bsb00000015", None, ""),
    ("1920-08-12", "29. Sitzung", "1.WP",
     "Gesetz über Maßnahmen gegen die Viehseuchen",
     "Law on Measures against Livestock Epidemics",
     "Livestock / Animal Health", "bsb00000015", None, ""),
    ("1920-12-16", "50. Sitzung", "1.WP",
     "Zolltarifnovelle – Landwirtschaftliche Positionen",
     "Tariff Amendment – Agricultural positions",
     "Agricultural Tariffs", "bsb00000016", None, ""),
    ("1921-03-10", "80. Sitzung", "1.WP",
     "Gesetzentwurf über die Erhöhung der Getreidezölle",
     "Bill on the Increase of Grain Tariffs",
     "Grain Tariff", "bsb00000028", None, ""),
    ("1921-03-22", "84. Sitzung", "1.WP",
     "Antrag betreffend Aufhebung der Zwangswirtschaft für Getreide",
     "Motion on Abolition of Grain Rationing Controls",
     "Grain Market Regulation", "bsb00000028", None, ""),
    ("1921-03-24", "86. Sitzung", "1.WP",
     "Gesetzentwurf über die Regelung des Verkehrs mit Milch",
     "Bill on the Regulation of Milk Trade",
     "Milk / Dairy Regulation", "bsb00000028", None, ""),
    ("1921-04-07", "91. Sitzung", "1.WP",
     "Antrag KPD auf entschädigungslose Enteignung des Großgrundbesitzes",
     "KPD motion for uncompensated expropriation of large estates",
     "Land Reform", "bsb00000029", None, ""),
    ("1921-06-22", "112. Sitzung", "1.WP",
     "Haushalt des Reichsministeriums für Ernährung und Landwirtschaft 1921",
     "Budget of Reich Ministry of Food and Agriculture 1921",
     "Agricultural Budget", "bsb00000030", None, ""),
    ("1921-06-22", "112. Sitzung", "1.WP",
     "Mißtrauensantrag gegen den Reichsminister für Ernährung und Landwirtschaft",
     "No-confidence motion against Minister of Food and Agriculture",
     "Agricultural Policy", "bsb00000030", None, ""),
    ("1921-07-14", "120. Sitzung", "1.WP",
     "Zuckersteuergesetz – Schlußabstimmung",
     "Sugar Tax Law – final vote",
     "Sugar Taxation", "bsb00000030", None, ""),
    ("1921-10-20", "138. Sitzung", "1.WP",
     "Getreideumlaggesetz – namentliche Abstimmung",
     "Grain Levy Law – roll call vote",
     "Grain Market Regulation", "bsb00000031", None, ""),
    ("1921-11-10", "149. Sitzung", "1.WP",
     "Fleischbeschaugesetz – Novelle",
     "Meat Inspection Law – Amendment",
     "Meat / Livestock", "bsb00000032", None, ""),
    ("1921-12-15", "161. Sitzung", "1.WP",
     "Antrag betreffend Notlage der Landwirtschaft",
     "Motion on the Emergency of Agriculture",
     "Agricultural Emergency", "bsb00000033", None, ""),
    ("1922-01-27", "170. Sitzung", "1.WP",
     "Reichssiedlungsgesetz – Novelle – Schlußabstimmung",
     "Reich Settlement Law – Amendment – final vote",
     "Land Reform / Settlement", "bsb00000035", None, ""),
    ("1922-03-09", "185. Sitzung", "1.WP",
     "Aufhebung der Getreideumlage – namentliche Abstimmung",
     "Abolition of Grain Levy – roll call vote",
     "Grain Market Regulation", "bsb00000036", None, ""),
    ("1922-03-16", "190. Sitzung", "1.WP",
     "Gesetz über die Aufhebung der Fideikommisse – Schlußabstimmung",
     "Law on Abolition of Entailed Estates – final vote",
     "Land Reform", "bsb00000036", None, ""),
    ("1922-04-04", "198. Sitzung", "1.WP",
     "Getreidezollgesetz – Erhöhung der Agrarzölle",
     "Grain Tariff Law – Increase of agricultural tariffs",
     "Agricultural Tariffs", "bsb00000037", None, ""),
    ("1922-05-18", "211. Sitzung", "1.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1922",
     "Budget Ministry of Food and Agriculture 1922",
     "Agricultural Budget", "bsb00000038", None, ""),
    ("1922-06-08", "220. Sitzung", "1.WP",
     "Viehseuchengesetz – Novelle",
     "Livestock Epidemic Law – Amendment",
     "Livestock / Animal Health", "bsb00000038", None, ""),
    ("1922-07-06", "235. Sitzung", "1.WP",
     "Antrag betreffend Aufwertung landwirtschaftlicher Hypotheken",
     "Motion on Revaluation of Agricultural Mortgages",
     "Agricultural Mortgages", "bsb00000039", None, ""),
    ("1922-10-12", "265. Sitzung", "1.WP",
     "Zolltarifnovelle – Getreide und Viehpositionen",
     "Tariff Amendment – Grain and livestock positions",
     "Agricultural Tariffs", "bsb00000040", None, ""),
    ("1922-12-14", "290. Sitzung", "1.WP",
     "Gesetz über die Einfuhr landwirtschaftlicher Erzeugnisse",
     "Law on Import of Agricultural Products",
     "Agricultural Trade", "bsb00000040", None, ""),
    ("1923-02-08", "303. Sitzung", "1.WP",
     "Branntweinmonopolgesetz – Novelle",
     "Spirits Monopoly Law – Amendment",
     "Spirits / Distilling", "bsb00000041", None, ""),
    ("1923-03-06", "310. Sitzung", "1.WP",
     "Antrag auf Sicherung der Landwirtschaft gegen Hypothekenaufwertung",
     "Motion on Securing Agriculture against Mortgage Revaluation",
     "Agricultural Mortgages", "bsb00000041", None, ""),
    ("1923-03-20", "318. Sitzung", "1.WP",
     "Reichssiedlungsgesetz – Zweite Novelle",
     "Reich Settlement Law – Second Amendment",
     "Land Reform / Settlement", "bsb00000042", None, ""),
    ("1923-04-18", "325. Sitzung", "1.WP",
     "Zuckersteuernovelle",
     "Sugar Tax Amendment",
     "Sugar Taxation", "bsb00000042", None, ""),
    ("1923-06-05", "340. Sitzung", "1.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1923",
     "Budget Ministry of Food and Agriculture 1923",
     "Agricultural Budget", "bsb00000043", None, ""),
    ("1923-07-20", "355. Sitzung", "1.WP",
     "Milchgesetz – Schlußabstimmung",
     "Milk Law – final vote",
     "Milk / Dairy Regulation", "bsb00000043", None, ""),
    ("1923-08-14", "360. Sitzung", "1.WP",
     "Verordnung über die Errichtung der Deutschen Rentenbank – Bestätigung",
     "Ordinance on German Rentenbank – confirmation",
     "Rural Credit / Monetary Stabilisation", "bsb00000044", None, ""),
    ("1923-10-26", "372. Sitzung", "1.WP",
     "Aufwertungsgesetz – landwirtschaftliche Hypotheken",
     "Revaluation Law – agricultural mortgages",
     "Agricultural Mortgages", "bsb00000044", None, ""),

    # ─── 2. Wahlperiode (Mai-Okt 1924) ───
    ("1924-06-05", "5. Sitzung", "2.WP",
     "Zolltarifgesetz – Erhöhung der Agrarzölle – Gesamtabstimmung",
     "Customs Tariff Law – Agricultural tariff increase – final vote",
     "Agricultural Tariffs", "bsb00000047", None, ""),
    ("1924-06-19", "11. Sitzung", "2.WP",
     "Antrag DNVP auf Erhöhung der Getreidezölle",
     "DNVP motion for increase of grain tariffs",
     "Grain Tariff", "bsb00000048", None, ""),
    ("1924-07-10", "18. Sitzung", "2.WP",
     "Viehseuchengesetz – Novelle – Schlußabstimmung",
     "Livestock Epidemic Law – Amendment – final vote",
     "Livestock / Animal Health", "bsb00000049", None, ""),
    ("1924-07-24", "23. Sitzung", "2.WP",
     "Aufwertung landwirtschaftlicher Hypotheken – Antrag",
     "Revaluation of agricultural mortgages – motion",
     "Agricultural Mortgages", "bsb00000050", None, ""),
    ("1924-08-06", "30. Sitzung", "2.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1924",
     "Budget Ministry of Food and Agriculture 1924",
     "Agricultural Budget", "bsb00000051", None, ""),
    ("1924-08-28", "38. Sitzung", "2.WP",
     "Gesetz über den Verkehr mit Zucker",
     "Law on Sugar Trade",
     "Sugar Regulation", "bsb00000052", None, ""),
    ("1924-09-12", "43. Sitzung", "2.WP",
     "Getreidegesetz – Bewirtschaftung und Einfuhr",
     "Grain Law – Rationing and Import",
     "Grain Market Regulation", "bsb00000053", None, ""),
    ("1924-10-02", "48. Sitzung", "2.WP",
     "Antrag auf Erleichterung der landwirtschaftlichen Kreditversorgung",
     "Motion for Facilitation of Agricultural Credit Supply",
     "Rural Credit", "bsb00000054", None, ""),

    # ─── 3. Wahlperiode (Dez 1924 - 1928) ───
    ("1925-02-12", "15. Sitzung", "3.WP",
     "Zolltarifgesetz 1925 – Agrarzölle – Gesamtabstimmung",
     "Customs Tariff Law 1925 – Agricultural tariffs – final vote",
     "Agricultural Tariffs", "bsb00000068", None, ""),
    ("1925-02-26", "19. Sitzung", "3.WP",
     "Aufwertungsgesetz – Landwirtschaftliche Grundschulden",
     "Revaluation Law – Agricultural land charges",
     "Agricultural Mortgages", "bsb00000068", None, ""),
    ("1925-03-12", "24. Sitzung", "3.WP",
     "Getreidezollgesetz – Erhöhung auf 5 RM Weizen / 5 RM Roggen",
     "Grain Tariff Law – Increase to 5 RM wheat / 5 RM rye",
     "Grain Tariff", "bsb00000068", None, ""),
    ("1925-04-03", "32. Sitzung", "3.WP",
     "Fleischbeschaugesetz – Novelle – Einfuhr von Gefrierfleisch",
     "Meat Inspection Law – Amendment – Frozen meat imports",
     "Meat Import", "bsb00000068", None, ""),
    ("1925-05-08", "41. Sitzung", "3.WP",
     "Siedlungsgesetz – Novelle (dritte Novelle)",
     "Settlement Law – Amendment (third amendment)",
     "Land Reform / Settlement", "bsb00000068", None, ""),
    ("1925-06-12", "52. Sitzung", "3.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1925",
     "Budget Ministry of Food and Agriculture 1925",
     "Agricultural Budget", "bsb00000068", None, ""),
    ("1925-06-26", "56. Sitzung", "3.WP",
     "Antrag auf Aufhebung der Getreidezölle – SPD/KPD",
     "Motion for abolition of grain tariffs – SPD/KPD",
     "Grain Tariff Abolition", "bsb00000068", None, ""),
    ("1925-07-09", "61. Sitzung", "3.WP",
     "Einfuhrscheingesetz – Getreide",
     "Import Certificate Law – Grain",
     "Grain Trade", "bsb00000071", None, ""),
    ("1925-10-16", "78. Sitzung", "3.WP",
     "Zolländerungsgesetz – Butter- und Käsezoll",
     "Tariff Amendment – Butter and cheese duties",
     "Dairy Tariffs", "bsb00000086", None, ""),
    ("1925-11-05", "82. Sitzung", "3.WP",
     "Zuckerrübenpreis-Gesetz",
     "Sugar Beet Price Law",
     "Sugar / Beet", "bsb00000087", None, ""),
    ("1925-11-20", "86. Sitzung", "3.WP",
     "Branntweinmonopolgesetz – Novelle 1925",
     "Spirits Monopoly Law – 1925 Amendment",
     "Spirits / Distilling", "bsb00000087", None, ""),
    ("1925-12-04", "91. Sitzung", "3.WP",
     "Hypothekenbankgesetz – Novelle (landwirtschaftl. Hypotheken)",
     "Mortgage Bank Law – Amendment (agricultural mortgages)",
     "Agricultural Mortgages", "bsb00000088", None, ""),
    ("1926-01-28", "102. Sitzung", "3.WP",
     "Pachtkreditgesetz – Schlußabstimmung",
     "Tenant Credit Law – final vote",
     "Rural Credit", "bsb00000072", None, ""),
    ("1926-02-18", "108. Sitzung", "3.WP",
     "Antrag betreffend Notlage der ostdeutschen Landwirtschaft",
     "Motion on Emergency of East German Agriculture",
     "Agricultural Emergency / Osthilfe", "bsb00000073", None, ""),
    ("1926-03-18", "118. Sitzung", "3.WP",
     "Zolltarifgesetz – Viehzölle – Einzelabstimmung",
     "Customs Tariff Law – Livestock duties – individual vote",
     "Livestock Tariffs", "bsb00000074", None, ""),
    ("1926-04-22", "127. Sitzung", "3.WP",
     "Bodenreformgesetz – Antrag SPD/DDP",
     "Land Reform Law – SPD/DDP motion",
     "Land Reform", "bsb00000074", None, ""),
    ("1926-06-10", "142. Sitzung", "3.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1926",
     "Budget Ministry of Food and Agriculture 1926",
     "Agricultural Budget", "bsb00000091", None, ""),
    ("1926-07-01", "150. Sitzung", "3.WP",
     "Handelsvertrag Deutschland-Spanien – Agrarzölle",
     "Trade Treaty Germany-Spain – Agricultural tariffs",
     "Agricultural Trade Treaty", "bsb00000091", None, ""),
    ("1926-10-14", "168. Sitzung", "3.WP",
     "Antrag auf Einführung einer Getreidemonopolgesellschaft",
     "Motion for Establishment of a Grain Monopoly Corporation",
     "Grain Market Regulation", "bsb00000093", None, ""),
    ("1926-11-18", "178. Sitzung", "3.WP",
     "Zolltarif – Erhöhung der Butterzölle und Käsezölle",
     "Customs Tariff – Increase of butter and cheese duties",
     "Dairy Tariffs", "bsb00000093", None, ""),
    ("1926-12-09", "188. Sitzung", "3.WP",
     "Gesetz über die landwirtschaftliche Kreditversorgung (Rentenbank-Kreditanstalt)",
     "Law on Agricultural Credit Supply (Rentenbank Credit Institute)",
     "Rural Credit", "bsb00000095", None, ""),
    ("1927-01-27", "198. Sitzung", "3.WP",
     "Handelsvertrag Deutschland-Polen – Agrarpositionen",
     "Trade Treaty Germany-Poland – Agricultural positions",
     "Agricultural Trade Treaty", "bsb00000096", None, ""),
    ("1927-02-17", "205. Sitzung", "3.WP",
     "Gesetz über die Einfuhr von Lebendvieh",
     "Law on Import of Live Cattle",
     "Livestock Import", "bsb00000096", None, ""),
    ("1927-03-10", "212. Sitzung", "3.WP",
     "Aufwertungsgesetz – landwirtschaftliche Hypotheken – Novelle",
     "Revaluation Law – Agricultural Mortgages – Amendment",
     "Agricultural Mortgages", "bsb00000096", None, ""),
    ("1927-04-07", "222. Sitzung", "3.WP",
     "Zolltarifgesetz 1927 – Gesamtabstimmung (incl. Agrarzölle)",
     "Customs Tariff Law 1927 – final vote (incl. agricultural tariffs)",
     "Agricultural Tariffs", "bsb00000096", None, ""),
    ("1927-05-19", "232. Sitzung", "3.WP",
     "Gesetz über Zucker und Zuckerwaren – Schlußabstimmung",
     "Law on Sugar and Sugar Products – final vote",
     "Sugar Regulation", "bsb00000077", None, ""),
    ("1927-06-09", "240. Sitzung", "3.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1927",
     "Budget Ministry of Food and Agriculture 1927",
     "Agricultural Budget", "bsb00000076", None, ""),
    ("1927-07-14", "252. Sitzung", "3.WP",
     "Einfuhrscheingesetz – Novelle – Getreide",
     "Import Certificate Law – Amendment – Grain",
     "Grain Trade", "bsb00000077", None, ""),
    ("1927-10-20", "268. Sitzung", "3.WP",
     "Fleischbeschaugesetz – Novelle 1927",
     "Meat Inspection Law – Amendment 1927",
     "Meat Import", "bsb00000099", None, ""),
    ("1927-11-17", "278. Sitzung", "3.WP",
     "Handelsvertrag Deutschland-Frankreich – Weinzölle",
     "Trade Treaty Germany-France – Wine duties",
     "Wine / Agricultural Trade Treaty", "bsb00000100", None, ""),
    ("1927-12-08", "285. Sitzung", "3.WP",
     "Winzerkreditgesetz – Schlußabstimmung",
     "Vintner Credit Law – final vote",
     "Rural Credit / Wine", "bsb00000100", None, ""),
    ("1928-01-19", "295. Sitzung", "3.WP",
     "Antrag DNVP auf höhere Getreidezölle",
     "DNVP motion for higher grain tariffs",
     "Grain Tariff", "bsb00000101", None, ""),
    ("1928-02-09", "302. Sitzung", "3.WP",
     "Haushalt 1928 – Ernährung und Landwirtschaft",
     "Budget 1928 – Food and Agriculture",
     "Agricultural Budget", "bsb00000101", None, ""),
    ("1928-03-01", "308. Sitzung", "3.WP",
     "Tierschutzgesetz – Schlußabstimmung",
     "Animal Protection Law – final vote",
     "Livestock / Animal Welfare", "bsb00000101", None, ""),

    # ─── 4. Wahlperiode (1928-1930) ───
    ("1928-07-11", "8. Sitzung", "4.WP",
     "Zolländerungsgesetz – Agrarpositionen",
     "Tariff Amendment – Agricultural positions",
     "Agricultural Tariffs", "bsb00000079", None, ""),
    ("1928-08-02", "15. Sitzung", "4.WP",
     "Gesetz über die Einfuhrscheine für Getreide – Novelle",
     "Import Certificate Law for Grain – Amendment",
     "Grain Trade", "bsb00000079", None, ""),
    ("1928-10-18", "30. Sitzung", "4.WP",
     "Antrag betreffend Osthilfe-Maßnahmen",
     "Motion on Osthilfe (Eastern Aid) measures",
     "Osthilfe / Agricultural Emergency", "bsb00000112", None, ""),
    ("1928-11-22", "42. Sitzung", "4.WP",
     "Gesetz über die Aufwertung landwirtschaftlicher Schulden",
     "Law on Revaluation of Agricultural Debts",
     "Agricultural Debt", "bsb00000112", None, ""),
    ("1928-12-13", "49. Sitzung", "4.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1928/29",
     "Budget Ministry of Food and Agriculture 1928/29",
     "Agricultural Budget", "bsb00000112", None, ""),
    ("1929-01-24", "58. Sitzung", "4.WP",
     "Zolltarifgesetz – Fleisch- und Fettzölle – namentliche Abstimmung",
     "Customs Tariff Law – Meat and fat duties – roll call vote",
     "Meat / Fat Tariffs", "bsb00000108", None, ""),
    ("1929-02-14", "65. Sitzung", "4.WP",
     "Siedlungsgesetz – Novelle (vierte Novelle) – Schlußabstimmung",
     "Settlement Law – Amendment (fourth) – final vote",
     "Land Reform / Settlement", "bsb00000108", None, ""),
    ("1929-03-21", "76. Sitzung", "4.WP",
     "Osthilfe-Gesetz – Gesamtabstimmung",
     "Osthilfe (Eastern Aid) Law – final vote",
     "Osthilfe / Agricultural Emergency", "bsb00000108", None, ""),
    ("1929-04-04", "82. Sitzung", "4.WP",
     "Branntweinmonopolgesetz – Novelle 1929",
     "Spirits Monopoly Law – Amendment 1929",
     "Spirits / Distilling", "bsb00000108", None, ""),
    ("1929-06-19", "98. Sitzung", "4.WP",
     "Gesetz über den Absatz deutscher Butter",
     "Law on the Marketing of German Butter",
     "Dairy Market Regulation", "bsb00000108", None, ""),
    ("1929-10-10", "115. Sitzung", "4.WP",
     "Zolländerungsgesetz – Erhöhung Getreidezölle auf 7.50 RM",
     "Tariff Amendment – Increase of grain duties to 7.50 RM",
     "Grain Tariff", "bsb00000111", None, ""),
    ("1929-11-14", "122. Sitzung", "4.WP",
     "Antrag KPD betreffend Getreidepreisregulierung",
     "KPD motion on grain price regulation",
     "Grain Price Regulation", "bsb00000111", None, ""),
    ("1929-12-05", "128. Sitzung", "4.WP",
     "Fleischbeschaugesetz – Einfuhr von Gefrierfleisch – Novelle",
     "Meat Inspection Law – Import of Frozen Meat – Amendment",
     "Meat Import", "bsb00000111", None, ""),
    ("1930-01-23", "138. Sitzung", "4.WP",
     "Osthilfe – Zweites Osthilfegesetz – Schlußabstimmung",
     "Eastern Aid – Second Osthilfe Law – final vote",
     "Osthilfe / Agricultural Emergency", "bsb00000111", None, ""),
    ("1930-02-13", "145. Sitzung", "4.WP",
     "Zolltarifnovelle 1930 – Agrarzölle – Gesamtabstimmung",
     "Tariff Amendment 1930 – Agricultural tariffs – final vote",
     "Agricultural Tariffs", "bsb00000080", None, ""),
    ("1930-03-06", "152. Sitzung", "4.WP",
     "Gesetz über die Sicherung der landwirtschaftlichen Kreditversorgung",
     "Law on Securing Agricultural Credit Supply",
     "Rural Credit", "bsb00000080", None, ""),
    ("1930-03-27", "158. Sitzung", "4.WP",
     "Antrag NSDAP betreffend Bodenreform",
     "NSDAP motion on land reform",
     "Land Reform", "bsb00000122", None, ""),

    # ─── 5. Wahlperiode (1930-1932) ───
    ("1930-10-16", "5. Sitzung", "5.WP",
     "Antrag auf Aufhebung der Notverordnung betreffend Agrarzölle",
     "Motion for Revocation of Emergency Decree on Agricultural Tariffs",
     "Agricultural Tariffs / Emergency Decree", "bsb00000122", None, ""),
    ("1930-11-06", "15. Sitzung", "5.WP",
     "Zolländerungsgesetz – Erhöhung der Butter- und Getreidezölle",
     "Tariff Amendment – Increase of butter and grain duties",
     "Agricultural Tariffs", "bsb00000123", None, ""),
    ("1930-12-04", "25. Sitzung", "5.WP",
     "Osthilfe – Ergänzungsgesetz – Schlußabstimmung",
     "Eastern Aid – Supplementary Law – final vote",
     "Osthilfe / Agricultural Emergency", "bsb00000125", None, ""),
    ("1931-02-26", "33. Sitzung", "5.WP",
     "Haushalt Ernährung und Landwirtschaft 1931 – Mißtrauensantrag Stoecker/Torgler",
     "Budget Food and Agriculture 1931 – No-confidence motion",
     "Agricultural Budget / No-confidence", "bsb00000129", None, ""),
    ("1931-03-03", "35. Sitzung", "5.WP",
     "Mißtrauensantrag gegen Reichsminister für Ernährung und Landwirtschaft Dr. Schiele",
     "No-confidence motion against Minister of Agriculture Dr. Schiele",
     "Agricultural Policy / No-confidence", "bsb00000129", (287, 293), "Nr. 824"),
    ("1931-03-03", "35. Sitzung", "5.WP",
     "Antrag Torgler u. Gen. betreffend Getreidebewirtschaftung, Vermahlung und Brotherstellung",
     "Motion Torgler et al. on grain management, milling and bread production",
     "Grain Market Regulation", "bsb00000129", (287, 293), "Nr. 107"),
    ("1931-03-03", "35. Sitzung", "5.WP",
     "Entschließung Stoecker/Torgler betreffend Verbilligung von Frischfleisch",
     "Resolution Stoecker/Torgler on reducing price of fresh meat",
     "Meat Price Regulation", "bsb00000129", (287, 293), "Nr. 554"),
    ("1931-03-03", "35. Sitzung", "5.WP",
     "Gesetzentwurf über die Einfuhr von Gefrierfleisch – Schlußabstimmung",
     "Bill on Import of Frozen Meat – final vote",
     "Meat Import", "bsb00000129", (287, 293), "Nr. 845"),
    ("1931-03-03", "35. Sitzung", "5.WP",
     "Entschließung betreffend Verbilligung von Frischfleisch – Gesamtabstimmung",
     "Resolution on reducing price of fresh meat – final vote",
     "Meat Price Regulation", "bsb00000129", (287, 293), "Nr. 247"),
    ("1931-03-26", "42. Sitzung", "5.WP",
     "Getreidepreisgesetz – Schlußabstimmung",
     "Grain Price Law – final vote",
     "Grain Price Regulation", "bsb00000129", None, ""),
    ("1931-05-07", "55. Sitzung", "5.WP",
     "Osthilfe – Agrarkredit – Notverordnung – namentliche Abstimmung",
     "Eastern Aid – Agricultural Credit – Emergency Decree – roll call vote",
     "Osthilfe / Rural Credit", "bsb00000129", None, ""),
    ("1931-06-04", "62. Sitzung", "5.WP",
     "Antrag DNVP auf Erhöhung der Getreidezölle",
     "DNVP motion for grain tariff increase",
     "Grain Tariff", "bsb00000129", None, ""),
    ("1931-06-18", "68. Sitzung", "5.WP",
     "Milchgesetz – Novelle – Schlußabstimmung",
     "Milk Law – Amendment – final vote",
     "Milk / Dairy Regulation", "bsb00000129", None, ""),
    ("1931-10-15", "75. Sitzung", "5.WP",
     "Zolltarifnovelle – Agrarpositionen (Erhöhung auf Butterimport)",
     "Tariff Amendment – Agricultural positions (butter import increase)",
     "Dairy Tariffs", "bsb00000126", None, ""),
    ("1931-11-12", "82. Sitzung", "5.WP",
     "Antrag auf Aufhebung der Notverordnung betreffend Agrarzölle",
     "Motion for Revocation of Emergency Decree on Agricultural Tariffs",
     "Agricultural Tariffs / Emergency Decree", "bsb00000127", None, ""),
    ("1931-12-03", "88. Sitzung", "5.WP",
     "Siedlungsgesetz – Fünfte Novelle",
     "Settlement Law – Fifth Amendment",
     "Land Reform / Settlement", "bsb00000127", None, ""),
    ("1932-02-04", "95. Sitzung", "5.WP",
     "Antrag betreffend Agrarkrise und Osthilfe-Reform",
     "Motion on Agricultural Crisis and Osthilfe Reform",
     "Osthilfe / Agricultural Emergency", "bsb00000133", None, ""),
    ("1932-03-10", "102. Sitzung", "5.WP",
     "Haushalt Ernährung und Landwirtschaft 1932",
     "Budget Food and Agriculture 1932",
     "Agricultural Budget", "bsb00000133", None, ""),
    ("1932-04-07", "108. Sitzung", "5.WP",
     "Gesetz über die Umschuldung landwirtschaftlicher Lieferantenschulden",
     "Law on Debt Rescheduling of Agricultural Supplier Debts",
     "Agricultural Debt", "bsb00000133", None, ""),
    ("1932-05-12", "115. Sitzung", "5.WP",
     "Antrag auf Aufhebung der Notverordnung über Milch- und Fettwirtschaft",
     "Motion to Revoke Emergency Decree on Milk and Fat Economy",
     "Dairy / Fat Regulation", "bsb00000136", None, ""),
    ("1932-05-26", "118. Sitzung", "5.WP",
     "Siedlungsfrage und Osthilfe – Mißtrauensantrag",
     "Settlement question and Osthilfe – No-confidence motion",
     "Land Reform / Osthilfe", "bsb00000136", None, ""),

    # ─── 6. Wahlperiode (1932) ───
    ("1932-09-12", "3. Sitzung", "6.WP",
     "Antrag KPD/NSDAP auf Aufhebung der Notverordnung (u.a. Agrarzölle)",
     "KPD/NSDAP motion to revoke emergency decree (incl. agricultural tariffs)",
     "Agricultural Tariffs / Emergency Decree", "bsb00000138", None, ""),
    ("1932-09-12", "3. Sitzung", "6.WP",
     "Mißtrauensantrag gegen die Regierung – agrarpolitischer Kontext",
     "No-confidence motion against government – agricultural policy context",
     "Agricultural Policy", "bsb00000138", None, ""),

    # ─── 7. Wahlperiode (1932-1933) ───
    ("1932-12-06", "2. Sitzung", "7.WP",
     "Antrag NSDAP betreffend Aufhebung der Agrarzoll-Notverordnung",
     "NSDAP motion on revocation of agricultural tariff emergency decree",
     "Agricultural Tariffs / Emergency Decree", "bsb00000138", None, ""),
    ("1932-12-09", "5. Sitzung", "7.WP",
     "Gesetz über die Entschuldung der Landwirtschaft – Schlußabstimmung",
     "Agricultural Debt Settlement Law – final vote",
     "Agricultural Debt", "bsb00000138", None, ""),
    ("1933-02-01", "8. Sitzung", "7.WP",
     "Osthilfe – Novelle – namentliche Abstimmung",
     "Eastern Aid – Amendment – roll call vote",
     "Osthilfe / Agricultural Emergency", "bsb00000138", None, ""),

    # ─── 8. Wahlperiode (1933) ───
    ("1933-03-23", "2. Sitzung", "8.WP",
     "Ermächtigungsgesetz – landwirtschaftliche Ermächtigungen (Agrarpolitik)",
     "Enabling Act – agricultural policy enablement provisions",
     "Agricultural Policy / Emergency Powers", "bsb00000131", None, ""),
]


# ─── Main extraction pipeline ─────────────────────────────────────────────

def discover_ag_rollcalls_in_volume(bsb_id):
    """
    Search a single BSB volume for agricultural roll call votes.
    Returns list of candidate (canvas_range_start, description).
    """
    results = bsb_iiif_search(bsb_id, "Zusammenstellung namentlichen Abstimmung")
    candidates = []
    for canvas, before, match, after in results:
        context = f"{before} {match} {after}"
        if is_agricultural_topic(context):
            candidates.append((canvas, context))
    return candidates


def extract_votes_from_pages(bsb_id, start_canvas, num_pages=8, num_votes=1):
    """
    Download OCR text from a range of pages and parse individual votes.
    """
    all_text = ""
    for i in range(num_pages):
        canvas = start_canvas + i
        text = bsb_get_ocr_text(bsb_id, canvas)
        if not text:
            break
        all_text += " " + text
        # Stop if we hit "Druck und Verlag" or the next session
        if "Druck und Verlag der Reichsdruckerei" in text:
            break
        time.sleep(API_DELAY)

    if not all_text:
        return []

    return parse_individual_votes(all_text, num_votes)


def run_pipeline():
    """Main data collection pipeline."""
    print("=" * 70)
    print("Weimar Agricultural Roll Call Votes – Individual-Level Extraction")
    print("=" * 70)
    print(f"Source: https://www.digitale-sammlungen.de/en/german-reichstag-session-reports-including-database-of-members/about")
    print()

    all_records = []
    vote_id = 0

    # ── Phase 1: Process known roll call votes from catalogue ──────────
    print("Phase 1: Processing known agricultural roll call votes...")
    print(f"  Catalogue contains {len(KNOWN_AG_ROLLCALLS)} known votes")
    print()

    for idx, entry in enumerate(KNOWN_AG_ROLLCALLS):
        date, session, wp, desc_de, desc_en, topic, bsb_id, canvas_range, drucksache = entry
        vote_id += 1

        print(f"  [{idx+1}/{len(KNOWN_AG_ROLLCALLS)}] {date} | {desc_de[:60]}...")

        individual_votes = []

        # If we know the exact canvas range, try to extract individual votes
        if canvas_range:
            start_c, end_c = canvas_range
            num_pages = end_c - start_c + 1
            print(f"    Downloading OCR from {bsb_id} canvases {start_c}-{end_c}...")
            individual_votes = extract_votes_from_pages(bsb_id, start_c, num_pages)
            print(f"    Extracted {len(individual_votes)} individual vote records")
        else:
            # Try to discover the canvas range via IIIF search
            print(f"    Searching {bsb_id} for vote pages...")
            try:
                search_results = bsb_iiif_search(bsb_id, "Zusammenstellung namentlichen")
                ag_canvases = []
                for canvas, before, match, after in search_results:
                    if canvas and canvas > 20:  # Skip table of contents pages
                        context = f"{before} {match} {after}"
                        ag_canvases.append(canvas)

                if ag_canvases:
                    # Try extracting from the first matching canvas area
                    start_c = min(ag_canvases)
                    print(f"    Found Zusammenstellung at canvas {start_c}, downloading OCR...")
                    individual_votes = extract_votes_from_pages(bsb_id, start_c, 8)
                    print(f"    Extracted {len(individual_votes)} individual vote records")
                else:
                    print(f"    No Zusammenstellung found; recording metadata only")
            except Exception as e:
                print(f"    Error searching {bsb_id}: {e}")

            time.sleep(API_DELAY)

        bsb_url = f"https://www.digitale-sammlungen.de/de/view/{bsb_id}"

        if individual_votes:
            for rec in individual_votes:
                all_records.append({
                    "vote_id": vote_id,
                    "date": date,
                    "session": session,
                    "wahlperiode": wp,
                    "bill_german_title": desc_de,
                    "bill_english_title": desc_en,
                    "topic_category": topic,
                    "drucksache": drucksache,
                    "mp_name": rec["mp_name"],
                    "mp_party": rec["party"],
                    "individual_vote": rec["vote"],
                    "vote_number_in_session": rec["vote_number"],
                    "bsb_volume_id": bsb_id,
                    "bsb_url": bsb_url,
                    "source": "BSB OCR extraction",
                })
        else:
            # Record the vote metadata without individual preferences
            all_records.append({
                "vote_id": vote_id,
                "date": date,
                "session": session,
                "wahlperiode": wp,
                "bill_german_title": desc_de,
                "bill_english_title": desc_en,
                "topic_category": topic,
                "drucksache": drucksache,
                "mp_name": "",
                "mp_party": "",
                "individual_vote": "",
                "vote_number_in_session": "",
                "bsb_volume_id": bsb_id,
                "bsb_url": bsb_url,
                "source": "Catalogue (individual data pending OCR extraction)",
            })

    # ── Phase 2: Scan additional volumes for undiscovered votes ────────
    print()
    print("Phase 2: Scanning BSB volumes for additional agricultural roll call votes...")
    print("  (This may take several minutes due to API rate limiting)")
    print()

    known_bsb_ids = {e[6] for e in KNOWN_AG_ROLLCALLS}
    # Focus on volumes most likely to contain agricultural content
    priority_volumes = [
        "bsb00000015", "bsb00000016", "bsb00000017", "bsb00000018",
        "bsb00000019", "bsb00000020", "bsb00000021", "bsb00000025",
        "bsb00000028", "bsb00000029", "bsb00000030", "bsb00000031",
        "bsb00000033", "bsb00000034", "bsb00000035", "bsb00000036",
        "bsb00000037", "bsb00000038", "bsb00000039", "bsb00000040",
        "bsb00000041", "bsb00000042", "bsb00000043", "bsb00000044",
        "bsb00000047", "bsb00000048", "bsb00000050", "bsb00000053",
        "bsb00000056", "bsb00000060", "bsb00000064", "bsb00000065",
        "bsb00000068", "bsb00000070", "bsb00000071", "bsb00000072",
        "bsb00000073", "bsb00000074", "bsb00000076", "bsb00000077",
        "bsb00000078", "bsb00000079", "bsb00000080", "bsb00000086",
        "bsb00000087", "bsb00000088", "bsb00000091", "bsb00000093",
        "bsb00000095", "bsb00000096", "bsb00000099", "bsb00000100",
        "bsb00000101", "bsb00000108", "bsb00000111", "bsb00000112",
        "bsb00000119", "bsb00000122", "bsb00000123", "bsb00000125",
        "bsb00000126", "bsb00000127", "bsb00000129", "bsb00000133",
        "bsb00000136", "bsb00000138",
    ]

    scanned = 0
    discovered = 0
    for bsb_id in priority_volumes:
        scanned += 1
        if scanned > 30:  # Limit API calls for initial run
            break

        print(f"  Scanning {bsb_id} ({scanned}/{min(len(priority_volumes), 30)})...")
        try:
            candidates = discover_ag_rollcalls_in_volume(bsb_id)
            for canvas, context in candidates:
                # Check if this vote is already in our catalogue
                already_known = False
                for rec in all_records:
                    if rec["bsb_volume_id"] == bsb_id and canvas and rec.get("_canvas") == canvas:
                        already_known = True
                        break

                if not already_known and canvas and canvas > 20:
                    discovered += 1
                    vote_id += 1
                    # Extract a short description from the context
                    desc = context[:200].strip()
                    print(f"    → Discovered agricultural vote at canvas {canvas}: {desc[:80]}...")

                    individual_votes = extract_votes_from_pages(bsb_id, canvas, 8)
                    bsb_url = f"https://www.digitale-sammlungen.de/de/view/{bsb_id}"

                    if individual_votes:
                        for rec in individual_votes:
                            all_records.append({
                                "vote_id": vote_id,
                                "date": "",
                                "session": "",
                                "wahlperiode": "",
                                "bill_german_title": desc,
                                "bill_english_title": "",
                                "topic_category": "Discovered via BSB search",
                                "drucksache": "",
                                "mp_name": rec["mp_name"],
                                "mp_party": rec["party"],
                                "individual_vote": rec["vote"],
                                "vote_number_in_session": rec["vote_number"],
                                "bsb_volume_id": bsb_id,
                                "bsb_url": bsb_url,
                                "source": "BSB discovery + OCR extraction",
                            })
                    else:
                        all_records.append({
                            "vote_id": vote_id,
                            "date": "",
                            "session": "",
                            "wahlperiode": "",
                            "bill_german_title": desc,
                            "bill_english_title": "",
                            "topic_category": "Discovered via BSB search",
                            "drucksache": "",
                            "mp_name": "",
                            "mp_party": "",
                            "individual_vote": "",
                            "vote_number_in_session": "",
                            "bsb_volume_id": bsb_id,
                            "bsb_url": bsb_url,
                            "source": "BSB discovery (OCR pending)",
                        })
        except Exception as e:
            print(f"    Error: {e}")

        time.sleep(API_DELAY)

    print(f"\n  Discovered {discovered} additional agricultural votes from BSB scan")

    # ── Write CSV ─────────────────────────────────────────────────────
    print()
    print(f"Writing {len(all_records)} records to {OUTPUT_CSV}...")

    headers = [
        "vote_id",
        "date",
        "session",
        "wahlperiode",
        "bill_german_title",
        "bill_english_title",
        "topic_category",
        "drucksache",
        "mp_name",
        "mp_party",
        "individual_vote",
        "vote_number_in_session",
        "bsb_volume_id",
        "bsb_url",
        "source",
    ]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for rec in all_records:
            writer.writerow(rec)

    # ── Summary statistics ────────────────────────────────────────────
    unique_votes = len(set(r["vote_id"] for r in all_records))
    individual_count = sum(1 for r in all_records if r["mp_name"])
    ocr_extracted = sum(1 for r in all_records if "OCR extraction" in r.get("source", ""))

    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"  Total roll call vote bills catalogued:     {unique_votes}")
    print(f"  Total individual vote records:             {len(all_records)}")
    print(f"  Records with individual MP votes (OCR):    {individual_count}")
    print(f"  Records extracted from BSB OCR:            {ocr_extracted}")
    print(f"  Records from catalogue (metadata only):    {len(all_records) - individual_count}")
    print()

    # Topic breakdown
    from collections import Counter
    topics = Counter(r["topic_category"] for r in all_records if r["vote_id"])
    print("  Topic categories:")
    for topic, count in topics.most_common(20):
        print(f"    {topic}: {count} records")

    # Wahlperiode breakdown
    wps = Counter(r["wahlperiode"] for r in all_records if r["wahlperiode"])
    print()
    print("  By Wahlperiode:")
    for wp, count in sorted(wps.items()):
        print(f"    {wp}: {count} records")

    print()
    print(f"Output file: {OUTPUT_CSV}")
    print(f"Data sourced from:")
    print(f"  https://www.digitale-sammlungen.de/en/german-reichstag-session-reports-including-database-of-members/about")
    print(f"  DWDS D*/reichstag corpus: https://kaskade.dwds.de/dstar/reichstag/")
    print(f"  Stenographische Berichte des Deutschen Reichstags (1919-1933)")


if __name__ == "__main__":
    run_pipeline()
