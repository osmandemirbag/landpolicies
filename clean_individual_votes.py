"""
Post-process the raw BSB OCR-extracted individual roll call vote data
to remove OCR noise and retain only plausible MP vote records.

This script reads the raw weimar_ag_individual_votes_master.csv produced
by create_individual_rollcall_data.py and outputs cleaned versions:
  - weimar_ag_individual_votes_master.csv  (overwritten, cleaned)
  - data/individual_by_year/ag_individual_YYYY.csv (overwritten, cleaned)

Cleaning strategy:
  1. Remove records matching known OCR noise patterns (parliamentary
     procedure terms, page headers, budget terminology, etc.)
  2. Remove records with names that are clearly sentence fragments
  3. Validate against pattern matching for German surname formats
  4. Keep only records where mp_name matches plausible name patterns

Data source: BSB Digitale Sammlungen (CC BY-SA 4.0)
"""

import csv
import os
import re
from collections import Counter, defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_CSV = os.path.join(SCRIPT_DIR, "weimar_ag_individual_votes_master.csv")
INDIVIDUAL_YEAR_DIR = os.path.join(SCRIPT_DIR, "data", "individual_by_year")

# ─── Comprehensive noise patterns ─────────────────────────────────────────
# These are words/phrases that appear in the OCR text of Reichstag
# Stenographische Berichte but are NOT MP names.

NOISE_EXACT = {
    # Page structure / headers
    "Reichstag", "Reichstags", "Reichs­", "Reichsregierung",
    "Nationalversammlung", "Sitzung", "Sitzungen",
    "Zusammenstellung", "Abstimmung", "Abstimmungen",
    "Namentliche Abstimmung", "Namentliche Abstimmungen",
    "Der Abstimmung", "Der Reichstag",
    "Druck­", "Drucksache", "Drucksachen",
    "Geschäftliches", "Geschäfts­",
    "Nächste Sitzung", "Seite Geschäftliches",
    "Stimmzettel", "Abgegebene",

    # Parliamentary procedure
    "Beratung", "Beschlußfassung", "Gesetzentwurf", "Antrag",
    "Entschließung", "Entschljetzungen", "Entwurf", "Entwurfs",
    "Novelle", "Schlußabstimmung", "Gesamtabstimmung",
    "Ausschuß", "Ausschusses", "Ausschusses: Der Reichstag",
    "Anlagen", "Anlage", "Anlagen II", "Anlagen)",
    "Genossen", "Anträge", "Beschlüsse", "Mündlichen", "Berichts",
    "Ermächtigung Gebrauch",

    # Budget / financial terminology
    "Haushalt", "Haushaltsgesetze", "Haushaltsentwurf",
    "Etat", "Etatstitel", "Einzelplan",
    "Rechnungsjahr", "Rechnungsjahres", "Rechnungs­",
    "Ausgaben", "Einnahmen", "Fortdauernde Ausgaben Kapitel",
    "Kap", "Kap. Tit", "Tit", "Titel",
    "Mark", "Pfennig", "Gramm",
    "Kapitel", "Posten",
    "Aufsteigende Gehälter: Gruppe",

    # Article / section references
    "Abs", "Ab­", "Artikel", "Ziffer", "Sitz. S",
    "RGBl", "III",

    # Agriculture / topic words (not names!)
    "Landwirtschaft", "Ernährung", "Getreide", "Milch", "Zucker",
    "Viehseuchen", "Siedlung", "Siedlungsgesetz", "Bodenreform",
    "Osthilfe", "Hypotheken", "Rentenbank", "Kreditversorgung",
    "Zolltarif", "Zolltarifgesetz", "Zolländerungsgesetz",
    "Branntweinmonopol", "Futtermitteln", "Rohzucker",
    "Gefrierfleisch", "Frischfleisch", "Fleisch",
    "Brvthersteltnng", "Brotberstellnng",  # OCR-corrupted words
    "Brotherstellung",
    "Einfuhr", "Ausfuhr", "Bevölkerung",
    "Bewirtschaftung", "Vermahlung", "Brotherstellung",

    # Days of week / months
    "Montag", "Dienstag", "Mittwoch", "Donnerstag",
    "Freitag", "Sonnabend", "Samstag", "Sonntag",
    "Januar", "Februar", "März", "April", "Mai", "Juni",
    "Juli", "August", "September", "Oktober", "November", "Dezember",

    # Common German words (not names)
    "Ich", "Wir", "Sie", "Er", "Es",
    "Die", "Der", "Das", "Den", "Dem", "Des",
    "Ein", "Eine", "Einer", "Eines", "Einem", "Einen",
    "Und", "Oder", "Aber", "Auch", "Nur", "Noch", "Schon", "Dann",
    "Diese", "Dieser", "Dieses", "Diesem", "Diesen",
    "Jene", "Jener", "Nach", "Vor", "Bei", "Mit",
    "Von", "Aus", "Für", "Auf", "In", "An­",
    "Über", "Unter", "Durch", "Gegen", "Ohne", "Zwischen", "Seit",
    "Meine", "Damen", "Herren", "Herr", "Herrn",
    "Sehr", "Hört", "Bravo", "Beifall", "Rufe", "Zuruf",
    "Davon", "Summe", "Bleiben", "Ungültig", "Name",
    "Druck", "Verlag", "Reichsdruckerei",
    "Berlin", "Norddeutschen", "Buchdruckerei", "Verlagsanstalt",
    "Wilhelmstraße",
    "Sozialisierung", "Sozialisierungsgesetz",
    "Demobilisation",
    "Volkswirtschaft", "Mißtrauensantrag",
    "Abgeordneten", "Abgeordnete", "Abgeordneter",
    "Vizepräsident", "Präsident", "Minister", "Reichsminister",
    "Reichsininister", "Reichsverkehrsminister", "Reichskanzler",
    "Parlament",

    # Budget structure terms
    "Tage", "Jahre", "Innern", "See­",
    "Zone", "Heft", "Staates",
    "Wahl­", "Verkehr, Luftfahrt",
    "Handwerkskultur", "Postver. Wallung",

    # More noise patterns found in data
    "Stoecker",  # appears as reference, not as voter name in this context
    "Torgler",   # often reference, not voter
}

# Regex patterns for strings that are clearly not MP names
NOISE_PATTERNS = [
    r"^[A-Z][a-z]{0,2}$",          # Single short word like "Ab", "An"
    r"^\d",                          # Starts with digit
    r"­$",                           # Ends with soft hyphen (broken word)
    r"^[A-Z][a-z]*­",               # Contains soft hyphen (broken word)
    r"Gruppe\s+X",                   # Budget group references
    r"Drucksachen",                  # Parliamentary documents
    r"Anlagen\)?:?\s",              # Appendix references
    r"^Herr\s+(Abgeordnet|Kolleg)", # "Herr Abgeordneter..."
    r"Reichs[­\-]?(?:tag|regierung|gesetzbl|vermögen)",
    r"(?:Gesetzbl|GeichS|Gesetzentwurf|Gesetzent)",
    r"^(?:Kap|Tit|Abs|Art)\b",     # Budget reference abbreviations
    r"(?:Sitz\.\s*S|Seite\s+Geschäftl)",
    r"Nächste\s+Sitzung",
    r"Namentliche\s+Abstimmung",
    r"Ausschuß|Ausschusses",
    r"Rechnungsjahr",
    r"Aufsteigende\s+Gehälter",
    r"Fortdauernde\s+Ausgaben",
    r"\bNr\b\.?\s*\d",              # Reference numbers like "Nr. 123"
    r"\bBd\.\s*\d",                  # Volume references
    r"\bS\.\s*\d",                   # Page references
    r"Geheimer\s+Regierungs",        # Civil service title in wrong context
    r"Oberverwaltungssekretär",
    r"Bibliotheksobersekretär",
    r"Verwaltungsinspektor",
    r"Berwaltungsinspektor",
    r"^\w+gesetz",                   # Words ending in -gesetz
    r"^\w+steuer",                   # Words ending in -steuer
    r"\bCode\s+Penal\b",
    r"Abgelehnt\b",
    r"Ablehnung\b",
    r"Ermächtigung\b",
    r"^Neubelastung",
    r"^Grund\s+von",
    r"Geschäfts?­",
]

# Compiled noise regex
NOISE_RE = [re.compile(p, re.IGNORECASE) for p in NOISE_PATTERNS]


def is_noise(name):
    """Check if a name is OCR noise rather than an MP name."""
    if not name or not name.strip():
        return True

    name = name.strip()

    # Check exact matches
    if name in NOISE_EXACT:
        return True

    # Check regex patterns
    for pat in NOISE_RE:
        if pat.search(name):
            return True

    # Names should not be very long (sentence fragments)
    if len(name) > 50:
        return True

    # Names should not contain certain characters
    if any(c in name for c in [':', ';', '=', '{', '}', '[', ']', '§', '/', '\\']):
        return True

    # Names should have at least one alphabetic character
    if not any(c.isalpha() for c in name):
        return True

    # Reject if name is all lowercase (German names are always capitalised)
    words = name.split()
    if words and words[0][0].islower() and words[0] not in ("von", "v.", "zu", "van", "de"):
        return True

    # Reject common German nouns that are NOT surnames
    COMMON_NOUNS = {
        "Beleidigung", "Gewährung", "Frankreich", "Imker", "Tausend",
        "Paradies", "Bemerkung", "Polemik", "Gegenstände", "Stande",
        "Stadtkreise", "Landkreise", "Monate", "Millionen", "Hundert",
        "Grundrente", "Pächter", "Zanuar", "Augenblick", "Klaffe",
        "Rohzucker", "Staatsbeihilfen", "Frage", "Hälfte", "Mehrheit",
        "Minderheit", "Grundsatz", "Verhandlung", "Verhandlungen",
        "Steuern", "Zölle", "Preise", "Preisen", "Interessen",
        "Vorschlag", "Vorschläge", "Vorschriften", "Bedingungen",
        "Verbesserung", "Einführung", "Erhaltung", "Verwaltung",
        "Förderung", "Forderung", "Forderungen", "Meldung",
        "Mitteilung", "Stellung", "Leistung", "Leistungen",
        "Wirkungen", "Maßnahmen", "Belange", "Beschwerden",
        "Grundlage", "Grundlagen", "Klärung", "Prüfung",
        "Genehmigung", "Feststellung", "Bestimmung", "Bestimmungen",
        "Ergebnis", "Ergebnisse", "Verfahren", "Verhältnis",
        "Verhältnisse", "Standpunkt", "Auffassung", "Erklärung",
        "Verpflichtung", "Verpflichtungen", "Zuständigkeit",
        "Notwendigkeit", "Möglichkeit", "Schwierigkeit",
        "Schwierigkeiten", "Voraussetzung", "Voraussetzungen",
        "Bedeutung", "Begründung", "Vorteil", "Vorteile",
        "Nachteil", "Nachteile", "Beispiel", "Zeichen",
        "Hunderte", "Tausende", "Braunsberg", "Königsberg",
        # More noise found in data
        "Zwangswirtschaft", "Anfragen", "Bordzulagen",
        "Maschinenzulagen", "Taucherzulagen", "Wohnung",
        "Erkenntnis", "Paragraph", "Zahlungsforderungen",
        "Gegebene", "Fraktion", "Seele", "Auswärtigen",
    }
    # Check first word against common nouns
    if words and words[0] in COMMON_NOUNS:
        return True

    # Reject names ending with common noun suffixes that are never surnames
    NOUN_SUFFIXES = (
        "schaft", "ung", "heit", "keit", "nis", "tion", "ieren",
        "lagen", "ungen", "nisse", "fragen", "nahmen", "werden",
        "ände", "asse", "essen", "ionen", "ieren", "ungen",
    )
    last_word = words[-1] if words else ""
    clean_last = last_word.rstrip(".,;:-")
    if clean_last and any(clean_last.endswith(s) for s in NOUN_SUFFIXES):
        # Except if it's after Dr. or Frau (could be a valid name)
        if not (words[0] in ("Dr.", "D.", "Frau", "Graf", "Frhr.", "Fhr.")):
            return True

    # Reject sentence fragments: "Name. Word" pattern
    if re.search(r"\w+\.\s+[A-Z]", name) and not name.startswith("Dr.") and not name.startswith("D."):
        if not re.match(r"^(Frau|Graf|Frhr?\.|Fürst)\s", name):
            return True

    # Reject names containing "Wahlperiode"
    if "Wahlperiode" in name:
        return True

    return False


def is_plausible_german_name(name):
    """Check if name follows patterns of German personal names.

    German MP names in Zusammenstellung follow these patterns:
      - Surname only: "Müller", "Schmidt"
      - With title: "Dr. Müller", "Frau Müller", "Graf Westarp"
      - With location: "Müller (Berlin)", "Schmidt (Hamburg)"
      - Compound: "von Oldenburg", "Frhr. von Richthofen"
      - Double name: "Müller-Franken"
    """
    name = name.strip()

    if not name:
        return False

    if is_noise(name):
        return False

    # Must start with uppercase letter or title prefix
    if not re.match(r"^(?:Dr\.|D\.|Frau|Frhr?\.|Graf|Fürst|von|v\.|[A-ZÄÖÜ])", name):
        return False

    # Should have reasonable length
    if len(name) < 3 or len(name) > 45:
        return False

    # Should not have too many words (max ~5 for a name with title + location)
    words = name.split()
    if len(words) > 6:
        return False

    # At least one word should look like a surname (capitalised, 3+ chars)
    has_surname = False
    for w in words:
        clean = w.strip("(). -,")
        if len(clean) >= 3 and clean[0].isupper() and clean.isalpha():
            has_surname = True
            break
    if not has_surname:
        # Check for common name patterns with dots/hyphens
        if re.match(r"^(?:Dr\.|D\.|Frau|Frhr?\.|Graf)\s+", name):
            has_surname = True

    return has_surname


def clean_mp_name(name):
    """Clean up an MP name string."""
    if not name:
        return ""

    # Remove leading/trailing dots, commas, spaces
    name = name.strip(" .,;:-–—()")

    # Remove OCR artifacts
    name = re.sub(r"\s*\.\s*\.\s*\.+", "", name)  # Remove "..."
    name = re.sub(r"\s+", " ", name)  # Normalise spaces

    # Remove trailing parenthetical if it doesn't look like a location
    # (locations are like "(Berlin)", "(Sachsen)")
    m = re.match(r"^(.+?)\s*\(([^)]+)\)\s*$", name)
    if m:
        loc = m.group(2)
        # Keep if it looks like a German place name
        if not re.match(r"^[A-ZÄÖÜ][a-zäöüß]+", loc):
            name = m.group(1).strip()

    return name


def main():
    print("=" * 70)
    print("Cleaning individual roll call vote data")
    print("=" * 70)

    # Save raw backup before cleaning
    RAW_BACKUP = os.path.join(SCRIPT_DIR, "weimar_ag_individual_votes_RAW.csv")
    if not os.path.exists(RAW_BACKUP):
        import shutil
        shutil.copy2(MASTER_CSV, RAW_BACKUP)
        print(f"  Raw backup saved: {RAW_BACKUP}")

    # Read raw data
    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        raw_rows = list(reader)
        headers = reader.fieldnames

    print(f"  Raw records read: {len(raw_rows)}")
    print(f"  Raw records with mp_name: {sum(1 for r in raw_rows if r['mp_name'])}")

    # ── Step 1: Clean names and filter noise ──────────────────────────
    cleaned = []
    removed_noise = 0
    removed_implausible = 0
    kept = 0
    metadata_only = 0

    for row in raw_rows:
        name = row.get("mp_name", "").strip()

        # Keep metadata-only rows (no mp_name)
        if not name:
            if row.get("source") == "catalogue_metadata_only":
                cleaned.append(row)
                metadata_only += 1
            continue

        # Clean the name
        name = clean_mp_name(name)

        # Check if noise
        if is_noise(name):
            removed_noise += 1
            continue

        # Check if plausible German name
        if not is_plausible_german_name(name):
            removed_implausible += 1
            continue

        # Keep this record with cleaned name
        row["mp_name"] = name
        cleaned.append(row)
        kept += 1

    print(f"\n  Cleaning results:")
    print(f"    Kept (plausible MPs):     {kept}")
    print(f"    Removed (noise):          {removed_noise}")
    print(f"    Removed (implausible):    {removed_implausible}")
    print(f"    Metadata-only rows:       {metadata_only}")
    print(f"    Total cleaned records:    {len(cleaned)}")

    # ── Step 1b: Remove names that appear across 3+ different parties
    #   (real MPs belong to at most 1-2 parties; cross-party names are noise)
    name_parties = defaultdict(set)
    for row in cleaned:
        if row["mp_name"] and row["mp_party"]:
            name_parties[row["mp_name"]].add(row["mp_party"])

    cross_party_noise = {name for name, parties in name_parties.items() if len(parties) >= 3}

    # Also build additional noise from remaining common noise words
    EXTRA_NOISE = {
        "Reichsrats", "Wochen", "Worte", "Überschrift", "Zeit", "Gesetz",
        "Satz", "Gesetzes", "Änderungen", "Aussprache", "Die Sitzung",
        "Geschäftsordnung", "Petitionen", "Gebiete", "Wirkung",
        "Tagesordnung", "Arbeitslosenversicherung", "Weiterberatung",
        "Reich", "Reichskanzlei", "Entschließungen", "Allgemeines",
        "Berichterstatter", "Kommission", "Abschnitt", "Wirtschaft",
        "Auswärtiges Amt", "Deutschen Reichs", "Reichs",
        "Kapitel Titel", "Übersicht, Summe Spalte",
        "Als Vorlagen", "Beamten", "Finanzen", "Nene",
        "Hoch",  # common word, not surname in context
        "Hundert Reichsmark Grundrente",
        "Aussetzung von Verfahren", "Vorjahren", "Zuschuß",
        "Antrag Nr", "FrauZillken",  # OCR merge artifact
        "Lobe",  # Reichstag president Löbe, not a voter
        "Dr. Stresemann Stücklen",  # two names merged
        "Landvolkpartei) Bachmann",  # party fragment merged with name
        "Dr. Wunderlich . Frau Wurm",  # two names merged
    }

    # Additional regex patterns for remaining noise
    EXTRA_NOISE_PATTERNS = [
        r"Verlag", r"Verordnung", r"Heiterkeit", r"Ministerialrat",
        r"Obermaschinenmeister", r"Bibliothek", r"Reichsbürgschaft",
        r"Reichsverfassung", r"Reichsvcr", r"Ruhegehalt", r"Wartegeld",
        r"Wohnzweck", r"Aushilfs", r"Allgemeinwohl", r"Unfug\b",
        r"Ungleichheit", r"Ursachen\b", r"Vorbemerkung", r"Schierigkeiten",
        r"Anlage\s+[IVXLC]", r"Für\s+Deutschland", r"^Diese\s+",
        r"^Satz\s+von", r"^Unter\s+Nr", r"Eisenbahn",
        r"^Hilfs$", r"^Dabei$", r"\bNr\b", r"Strafgesetzbuch",
        r"Rechtsanwalt", r"Gerichtsvollzieher", r"Oberpostmeister",
        r"Regierungsrat", r"Sekretäre?\b", r"Obersekretär",
        r"Inspektor", r"^Für\s+", r"^Carl\s+\w+\s+Verlag",
        r"^\w+empfänger$", r"^\w+meister$",
        # More institutional / procedural terms
        r"Reichswehr", r"Reichsarbeit", r"Reichshaushalts",
        r"Reichsmarine", r"Reichsfinanz", r"Reichspost",
        r"Reichsjustiz", r"Besoldungen", r"Volksvcrmögens",
        r"Milliarden", r"Goldmark", r"Steuerfragen",
        r"Optionsfragen", r"Asyl\b", r"Zakobstra",
        r"Abgeordneter?\.\)", r"Abgeordneten\s+\w+",
        r"Einzelgehalt", r"Pachtperioden", r"Lizenzen",
        r"Hauptleute", r"Heeresleitung", r"Heilzwecke",
        r"Entente", r"Diskussion\b", r"Aussicht\b",
        r"Kilogramm", r"Kurse\s+von", r"Öffentlichkeit",
        r"Wiederverwendung", r"Übungsplätzen",
        r"Anm\.\.", r"Abschnitte\b", r"Einzelgehalt",
        r"Fieberthermometer", r"Ostpreußen\b",
        r"Regierung\b$", r"Zustimmung\b$", r"Abkommen\b$",
        r"Anspruch\b$", r"Gegenstand\b",
        r"^\w+ministerium$", r"Titel\b$",
        r"^Alte\s+", r"^Als\s+V$", r"^Alk\s+",
        r"Monats\b$", r"Arbeiter\b$", r"Schätzt\b$",
        r"^Die\s+\w+isation$", r"^Die\s+\w+lung$",
        r"^Die\s+Anlage$", r"Moskau\b", r"Pfennig\b",
        r"Bauernpartei\b", r"Marken\b$",
    ]
    EXTRA_NOISE_RE = [re.compile(p, re.IGNORECASE) for p in EXTRA_NOISE_PATTERNS]

    cleaned2 = []
    removed_crossparty = 0
    removed_extra = 0
    for row in cleaned:
        name = row.get("mp_name", "")
        if not name:
            cleaned2.append(row)
            continue
        if name in cross_party_noise:
            removed_crossparty += 1
            continue
        if name in EXTRA_NOISE:
            removed_extra += 1
            continue
        # Check extra regex patterns
        extra_match = False
        for pat in EXTRA_NOISE_RE:
            if pat.search(name):
                extra_match = True
                break
        if extra_match:
            removed_extra += 1
            continue
        cleaned2.append(row)

    print(f"\n  Cross-party noise removal:")
    print(f"    Names appearing in 3+ parties: {len(cross_party_noise)}")
    print(f"    Records removed (cross-party): {removed_crossparty}")
    print(f"    Records removed (extra noise): {removed_extra}")
    print(f"    Records remaining:             {len(cleaned2)}")

    cleaned = cleaned2

    # ── Step 2: Show quality stats ────────────────────────────────────
    mp_records = [r for r in cleaned if r["mp_name"]]
    name_counts = Counter(r["mp_name"] for r in mp_records)
    party_counts = Counter(r["mp_party"] for r in mp_records)

    print(f"\n  Quality stats after cleaning:")
    print(f"    Unique MP names:          {len(name_counts)}")
    print(f"    Records with party:       {sum(1 for r in mp_records if r['mp_party'])}")
    print(f"    Records without party:    {sum(1 for r in mp_records if not r['mp_party'])}")

    print(f"\n  Party distribution:")
    for party, count in sorted(party_counts.items(), key=lambda x: -x[1]):
        if party:
            print(f"    {party:12s}: {count:5d}")
        else:
            print(f"    {'(unknown)':12s}: {count:5d}")

    print(f"\n  Top 25 most frequent MP names:")
    for name, count in name_counts.most_common(25):
        # Find associated party
        parties = set(r["mp_party"] for r in mp_records if r["mp_name"] == name and r["mp_party"])
        party_str = "/".join(sorted(parties)) if parties else "?"
        print(f"    {count:4d}  {party_str:12s}  {name}")

    # ── Step 3: Write cleaned master CSV ──────────────────────────────
    print(f"\n  Writing cleaned master CSV: {MASTER_CSV}")
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in cleaned:
            writer.writerow(row)

    # ── Step 4: Write year-by-year CSVs ───────────────────────────────
    os.makedirs(INDIVIDUAL_YEAR_DIR, exist_ok=True)

    by_year = defaultdict(list)
    for row in cleaned:
        by_year[int(row["year"])].append(row)

    print(f"\n  Writing year-by-year CSVs to {INDIVIDUAL_YEAR_DIR}/")
    for year in sorted(by_year):
        fname = os.path.join(INDIVIDUAL_YEAR_DIR, f"ag_individual_{year}.csv")
        with open(fname, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in by_year[year]:
                writer.writerow(row)
        n_mp = sum(1 for r in by_year[year] if r["mp_name"])
        n_bills = len(set(r["vote_id"] for r in by_year[year]))
        print(f"    {year}: {n_bills:3d} bills, {n_mp:5d} individual vote records")

    print(f"\n  Done!")
    print(f"  Output: {MASTER_CSV}")
    print(f"  Output: {INDIVIDUAL_YEAR_DIR}/ag_individual_YYYY.csv")


if __name__ == "__main__":
    main()
