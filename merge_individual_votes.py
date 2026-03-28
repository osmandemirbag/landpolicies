"""
Merge all year-by-year individual vote CSVs into a single master CSV.

This script reads data/individual_by_year/ag_individual_YYYY.csv files
(produced by extract_year_votes.py) and combines them into:
  - weimar_ag_individual_votes_master.csv (merged master)

It also applies the cleaning logic from clean_individual_votes.py
to filter OCR noise.

Usage:
    python merge_individual_votes.py
    python merge_individual_votes.py --no-clean   # skip cleaning step

Data source: BSB Digitale Sammlungen (CC BY-SA 4.0)
"""

import argparse
import csv
import glob
import os
import re
from collections import Counter, defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INDIVIDUAL_YEAR_DIR = os.path.join(SCRIPT_DIR, "data", "individual_by_year")
MASTER_CSV = os.path.join(SCRIPT_DIR, "weimar_ag_individual_votes_master.csv")

CSV_HEADERS = [
    "vote_id", "date", "year", "session", "wahlperiode",
    "bill_german_title", "bill_english_title", "topic_category",
    "drucksache", "mp_name", "mp_party", "vote", "vote_column",
    "bsb_volume_id", "bsb_url", "source",
]

# ─── Comprehensive noise patterns for cleaning ────────────────────────────

NOISE_EXACT = {
    "Reichstag", "Reichstags", "Reichs­", "Reichsregierung",
    "Nationalversammlung", "Sitzung", "Sitzungen",
    "Zusammenstellung", "Abstimmung", "Abstimmungen",
    "Namentliche Abstimmung", "Namentliche Abstimmungen",
    "Der Abstimmung", "Der Reichstag",
    "Druck­", "Drucksache", "Drucksachen",
    "Geschäftliches", "Geschäfts­",
    "Nächste Sitzung", "Seite Geschäftliches",
    "Stimmzettel", "Abgegebene",
    "Beratung", "Beschlußfassung", "Gesetzentwurf", "Antrag",
    "Entschließung", "Entschljetzungen", "Entwurf", "Entwurfs",
    "Novelle", "Schlußabstimmung", "Gesamtabstimmung",
    "Ausschuß", "Ausschusses", "Ausschusses: Der Reichstag",
    "Anlagen", "Anlage", "Anlagen II", "Anlagen)",
    "Genossen", "Anträge", "Beschlüsse", "Mündlichen", "Berichts",
    "Ermächtigung Gebrauch",
    "Haushalt", "Haushaltsgesetze", "Haushaltsentwurf",
    "Etat", "Etatstitel", "Einzelplan",
    "Rechnungsjahr", "Rechnungsjahres", "Rechnungs­",
    "Ausgaben", "Einnahmen", "Fortdauernde Ausgaben Kapitel",
    "Kap", "Kap. Tit", "Tit", "Titel",
    "Mark", "Pfennig", "Gramm",
    "Kapitel", "Posten",
    "Aufsteigende Gehälter: Gruppe",
    "Abs", "Ab­", "Artikel", "Ziffer", "Sitz. S",
    "RGBl", "III",
    "Landwirtschaft", "Ernährung", "Getreide", "Milch", "Zucker",
    "Viehseuchen", "Siedlung", "Siedlungsgesetz", "Bodenreform",
    "Osthilfe", "Hypotheken", "Rentenbank", "Kreditversorgung",
    "Zolltarif", "Zolltarifgesetz", "Zolländerungsgesetz",
    "Branntweinmonopol", "Futtermitteln", "Rohzucker",
    "Gefrierfleisch", "Frischfleisch", "Fleisch",
    "Brvthersteltnng", "Brotberstellnng",
    "Brotherstellung",
    "Einfuhr", "Ausfuhr", "Bevölkerung",
    "Bewirtschaftung", "Vermahlung",
    "Montag", "Dienstag", "Mittwoch", "Donnerstag",
    "Freitag", "Sonnabend",
    "Januar", "Februar", "März", "April", "Mai", "Juni",
    "Juli", "August", "September", "Oktober", "November", "Dezember",
    "Vizepräsident", "Präsident", "Minister", "Reichsminister",
    "Reichsininister", "Reichsverkehrsminister", "Reichskanzler",
    "Meine Damen", "Meine Herren",
    "Hört", "Sehr", "Bravo", "Rufe", "Beifall",
    "Berlin", "Norddeutschen", "Buchdruckerei", "Verlagsanstalt",
    "Wilhelmstraße", "Reichsdruckerei",
}

NOISE_PATTERNS = [
    r"^\d+$",
    r"^\d+\.\s*$",
    r"^(Sitz|Druck|Verlag|Kapitel|Titel|Seite)\b",
    r"^(Namentliche|Zusammenstellung|Abgegebene|Ungültige|Gültige)\b",
    r"^(Bleiben|Summe|Davon|Ergibt)\b",
    r"^(Drucksachen|Anlagen)\b",
    r"(Reichstag|Nationalversammlung|Abstimmung)",
    r"^[IVX]+\.\s",
    r"^\(\d",
]


def is_noise_record(name):
    """Check if an mp_name looks like OCR noise."""
    if not name or not name.strip():
        return True

    name = name.strip()

    if name in NOISE_EXACT:
        return True

    for pat in NOISE_PATTERNS:
        if re.search(pat, name):
            return True

    # Too short
    if len(name) < 3:
        return True

    # All digits
    if name.replace(" ", "").replace(".", "").isdigit():
        return True

    return False


def main():
    parser = argparse.ArgumentParser(
        description="Merge year-by-year individual vote CSVs into master CSV")
    parser.add_argument("--no-clean", action="store_true",
                        help="Skip noise-cleaning step")
    args = parser.parse_args()

    print("=" * 60)
    print("Merging individual vote CSVs into master file")
    print("=" * 60)

    # Find all year CSVs
    pattern = os.path.join(INDIVIDUAL_YEAR_DIR, "ag_individual_*.csv")
    year_files = sorted(glob.glob(pattern))

    if not year_files:
        print(f"  No year files found in {INDIVIDUAL_YEAR_DIR}/")
        print(f"  Run extract_year_votes.py first.")
        return

    print(f"  Found {len(year_files)} year files")

    # Read all records
    all_records = []
    for fpath in year_files:
        year_label = os.path.basename(fpath).replace("ag_individual_", "").replace(".csv", "")
        with open(fpath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        n_mp = sum(1 for r in rows if r.get("mp_name", "").strip())
        n_bills = len(set(r["vote_id"] for r in rows))
        print(f"    {year_label}: {n_bills} bills, {n_mp} individual records")
        all_records.extend(rows)

    print(f"\n  Total raw records: {len(all_records)}")

    # Apply cleaning
    if not args.no_clean:
        cleaned = []
        removed = 0
        for rec in all_records:
            name = rec.get("mp_name", "").strip()
            if name and is_noise_record(name):
                removed += 1
                continue
            cleaned.append(rec)

        print(f"  Noise records removed: {removed}")
        print(f"  Clean records: {len(cleaned)}")
        all_records = cleaned

    # Write master CSV
    print(f"\n  Writing master CSV: {MASTER_CSV}")
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for rec in all_records:
            # Ensure all fields exist
            row = {h: rec.get(h, "") for h in CSV_HEADERS}
            writer.writerow(row)

    # Summary
    records_with_mp = [r for r in all_records if r.get("mp_name", "").strip()]
    n_total = len(all_records)
    n_mp = len(records_with_mp)
    n_bills = len(set(r["vote_id"] for r in all_records))

    by_year = defaultdict(int)
    for r in records_with_mp:
        by_year[r["year"]] += 1

    print(f"\n  Summary:")
    print(f"    Total records:     {n_total}")
    print(f"    Individual votes:  {n_mp}")
    print(f"    Bills covered:     {n_bills}")
    print(f"\n  Records by year:")
    for year in sorted(by_year):
        print(f"    {year}: {by_year[year]}")

    print(f"\n  Output: {MASTER_CSV}")
    print(f"  Data source: BSB Digitale Sammlungen (CC BY-SA 4.0)")


if __name__ == "__main__":
    main()
