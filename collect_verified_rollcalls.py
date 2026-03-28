"""
Collect VERIFIED Weimar Reichstag roll call votes on agricultural bills.

This script:
1. Scans each BSB volume for ACTUAL "Zusammenstellung" (tally summary) pages
   by searching the OCR text for the structured header format.
2. For each found Zusammenstellung, extracts the date, session, bill references,
   and aggregate vote counts.
3. Maps tally pages to agricultural bills by matching bill titles/Drucksache
   references against the catalogue.
4. For confirmed agricultural roll calls, extracts individual MP vote data
   from the adjacent vote list pages.
5. Outputs a verified, clean dataset.

Key insight: The Weimar Reichstag often bundled multiple bills into a single
"namentliche Abstimmung" with numbered columns (1-5). The same MP name list
and vote appears under all bundled bills. This is historically correct, not
a data error.

Data sources:
  - BSB Digitale Sammlungen IIIF + OCR API (CC BY-SA 4.0)
  - Existing bill catalogue from create_individual_rollcall_data.py

Output:
  - weimar_ag_rollcall_verified.csv       (verified bill-level catalogue)
  - weimar_ag_individual_votes_clean.csv  (clean individual vote data)
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
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
HTTP_TIMEOUT = 30
API_DELAY = 0.4

# ── BSB API helpers ──────────────────────────────────────────────────────

def http_get(url, retries=2):
    """HTTP GET with retries."""
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            resp = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
            return resp.read()
        except Exception as e:
            if attempt == retries:
                return None
            time.sleep(1)
    return None


def bsb_search(bsb_id, query):
    """Search within a BSB volume using IIIF Search API.
    Returns list of (canvas_number, context_text)."""
    url = (f"https://api.digitale-sammlungen.de/iiif/services/search/v1/"
           f"{bsb_id}?q={urllib.parse.quote(query)}")
    results = []
    for _ in range(3):
        raw = http_get(url)
        if not raw:
            break
        data = json.loads(raw)
        canvas_map = {}
        for r in data.get("resources", []):
            on = r.get("on", "")
            m = re.search(r"canvas/(\d+)", on)
            if m:
                canvas_map[r.get("@id", "")] = int(m.group(1))
        for h in data.get("hits", []):
            before = h.get("before", "")
            match = h.get("match", "")
            after = h.get("after", "")
            canvas = None
            for a in h.get("annotations", []):
                if a in canvas_map:
                    canvas = canvas_map[a]
                    break
            results.append((canvas, f"{before} {match} {after}"))
        next_url = data.get("next")
        if next_url:
            url = next_url
            time.sleep(API_DELAY)
        else:
            break
    return results


def bsb_get_ocr(bsb_id, canvas):
    """Download hOCR for a page and return plain text."""
    url = f"https://api.digitale-sammlungen.de/ocr/{bsb_id}/{canvas}"
    raw = http_get(url)
    if not raw:
        return ""
    html = raw.decode('utf-8', errors='replace')
    words = re.findall(r'class="ocrx_word"[^>]*>([^<]+)', html)
    return " ".join(words)


def bsb_get_manifest(bsb_id):
    """Get BSB IIIF manifest (volume label and page count)."""
    url = f"https://api.digitale-sammlungen.de/iiif/presentation/v2/{bsb_id}/manifest"
    raw = http_get(url)
    if not raw:
        return '', 0
    data = json.loads(raw)
    label = data.get('label', '')
    canvases = data.get('sequences', [{}])[0].get('canvases', [])
    return label, len(canvases)


# ── Roll call page structure ──────────────────────────────────────────────
# A typical "namentliche Abstimmung" in the Stenographische Berichte:
#   Page N:   "Namentliche Abstimmungen in der XX. Sitzung am [date]"
#             Lists each bill voted on (numbered 1-5)
#   Pages N+1 to N+k: MP names with vote columns (1-5 matching bills)
#   Last page: "Zusammenstellung" with aggregate vote counts
#
# We search for the header page first, then read surrounding pages.

def find_tally_pages(bsb_id):
    """Find Zusammenstellung (tally) pages in a BSB volume.
    These are the summary pages at the end of each roll call vote listing
    vote totals. Returns list of canvas numbers (excluding index pages)."""

    # "Zusammenstellung Abgegebene Stimmzettel" is highly specific to actual
    # tally pages, much better than general "Zusammenstellung" searches
    hits = bsb_search(bsb_id, "Zusammenstellung Abgegebene Stimmzettel")
    canvases = set()
    for canvas, ctx in hits:
        if canvas and canvas > 10:  # Skip index/TOC pages
            canvases.add(canvas)
    return sorted(canvases)


def find_header_from_tally(bsb_id, tally_canvas, max_lookback=15):
    """Given a tally page, look backward to find the roll call header page.
    The header page has numbered bill items '1. über den/die/das...'
    Returns (header_canvas, header_text) or (None, None)."""

    for offset in range(1, max_lookback + 1):
        candidate = tally_canvas - offset
        if candidate <= 10:
            break
        text = bsb_get_ocr(bsb_id, candidate)
        time.sleep(API_DELAY)
        if not text:
            continue
        # Check for numbered bill list (unique to roll call headers)
        if re.search(r'1\.\s+über\s+', text):
            return candidate, text
    return None, None


def parse_rollcall_header(text):
    """Parse a Namentliche Abstimmung header page to extract:
    - date, session number
    - list of bills voted on (with Drucksache numbers)
    Returns dict or None if not a header page.

    A genuine header page has: 'Namentliche Abstimmung(en) in der XX. Sitzung'
    followed by numbered bill items '1. über den...', '2. über den...'
    This distinguishes it from table of contents or debate pages."""

    # Must have the numbered bill list to be a genuine header
    if not re.search(r'1\.\s+über\s+', text):
        return None

    # Must have Namentliche Abstimmung
    if not re.search(r'Namentliche\s+Abstimmung', text):
        return None

    # Extract date from the page header "XX. Sitzung. [day] den DD. Month YYYY"
    month_map = {
        'Januar': '01', 'Februar': '02', 'März': '03', 'April': '04',
        'Mai': '05', 'Juni': '06', 'Juli': '07', 'August': '08',
        'September': '09', 'Oktober': '10', 'November': '11', 'Dezember': '12'
    }
    month_regex = '|'.join(month_map.keys())

    # Find date from the page header (more reliable than the "in der XX. Sitzung" line
    # which may have OCR errors in the session number)
    date_m = re.search(
        rf'(\d{{1,3}})\.\s*Sitzung\.\s+\w+\s+(?:den|dem)\s+(\d{{1,2}})\.\s*'
        rf'({month_regex})\s*(\d{{4}})',
        text
    )
    if not date_m:
        return None

    session_nr = date_m.group(1)
    day = date_m.group(2)
    month_name = date_m.group(3)
    year = date_m.group(4)
    month = month_map.get(month_name, '00')
    date_str = f"{year}-{month}-{day.zfill(2)}"

    # Extract numbered bill references
    # Pattern: "N. über den/die/das [description] — Nr. XXX der Drucksachen —"
    # The text between numbered items is the bill description
    bills = []

    # Find all "N. über" markers and "N. Abstimmung" (which ends the list)
    markers = list(re.finditer(r'(\d)\.\s+über\s+', text))
    end_marker = re.search(r'\d\.\s*(?:\d\.\s*)*Abstimmung', text)

    for i, marker in enumerate(markers):
        bill_nr = int(marker.group(1))
        start = marker.start()

        # End of this bill's text is either the next bill or the "Abstimmung" line
        if i + 1 < len(markers):
            end = markers[i + 1].start()
        elif end_marker:
            end = end_marker.start()
        else:
            end = min(start + 500, len(text))

        bill_text = text[start:end].strip()

        # Extract Drucksache number
        drucksache_nums = re.findall(r'Nr[.,]\s*(\d+)\s*(?:der\s+Drucksache|$)', bill_text)
        drucksache = drucksache_nums[0] if drucksache_nums else ''

        # Clean up description
        desc = bill_text
        # Remove the leading "N. über den/die/das"
        desc = re.sub(r'^\d\.\s+über\s+(?:den|die|das)\s+', '', desc)
        # Remove Drucksache reference
        desc = re.sub(r'\s*—\s*Nr[.,]\s*\d+\s*(?:der\s+)?Drucksache[n]?\s*—?\s*', ' ', desc)
        # Remove "bei der Beratung des..." context
        desc = re.sub(r'\s*bei\s+der\s+Beratung\s+des\s+.*', '', desc, flags=re.DOTALL)
        # Remove "Schlußabstimmung" parenthetical but keep it as a flag
        is_final = 'Schlußabstimmung' in desc or 'Schlnßabstimmnng' in desc
        desc = re.sub(r'\s*\(Schl[uü]ß?abstimm[un]*ng\)', '', desc)
        # Clean whitespace
        desc = re.sub(r'\s+', ' ', desc).strip()

        if len(desc) > 10:  # Skip empty/too-short descriptions
            bills.append({
                'column': bill_nr,
                'description': desc[:300],
                'drucksache': drucksache,
                'is_final_vote': is_final,
            })

    if not bills:
        return None

    return {
        'date': date_str,
        'session': f"{session_nr}. Sitzung",
        'year': int(year),
        'bills': bills,
    }


def parse_zusammenstellung(text):
    """Parse a Zusammenstellung page to extract aggregate vote counts.
    Returns dict with vote totals or None."""

    if 'Zusammenstellung' not in text:
        return None

    result = {}

    # Look for "Ja XXX" and "Nein XXX" patterns
    ja_m = re.search(r'Ja\s+(\d+)', text)
    nein_m = re.search(r'Nein\s+(\d+)', text)
    enthalten_m = re.search(r'[Ee]nthalten\s+(\d+)', text)
    stimmzettel_m = re.search(r'[Aa]bgegebene\s+Stimmzettel\s+(\d+)', text)

    if ja_m:
        result['vote_yes'] = int(ja_m.group(1))
    if nein_m:
        result['vote_no'] = int(nein_m.group(1))
    if enthalten_m:
        result['vote_abstain'] = int(enthalten_m.group(1))
    if stimmzettel_m:
        result['total_ballots'] = int(stimmzettel_m.group(1))

    return result if result else None


# ── Agricultural bill matching ────────────────────────────────────────────

# Keywords that indicate an agricultural bill
AG_KEYWORDS_LOWER = [
    'landwirtschaft', 'ernährung', 'agrar', 'getreide', 'vieh', 'fleisch',
    'milch', 'butter', 'käse', 'zucker', 'siedlung', 'bodenreform',
    'hypothek', 'osthilfe', 'großgrundbesitz', 'fideikommiss', 'branntwein',
    'rentenbank', 'pachtkr', 'einfuhrschein', 'zuckerrüb', 'fettwirtschaft',
    'winzer', 'weinzöll', 'tierschutz', 'lebendvieh', 'gefrierfleisch',
    'frischfleisch', 'zoll.*landwirt', 'zoll.*agrar', 'zoll.*getreide',
    'zoll.*vieh', 'zoll.*butter', 'zolltarif.*agrar',
]


def is_agricultural_bill(description):
    """Check if a bill description is agricultural."""
    desc_lower = description.lower()
    for kw in AG_KEYWORDS_LOWER:
        if re.search(kw, desc_lower):
            return True
    return False


# ── Load existing catalogue ──────────────────────────────────────────────

def load_existing_catalogue():
    """Load the existing KNOWN_AG_ROLLCALLS from create_individual_rollcall_data.py"""
    csv_path = os.path.join(SCRIPT_DIR, "weimar_ag_rollcall_master.csv")
    if not os.path.exists(csv_path):
        return {}

    catalogue = {}
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row['date'], row['session'])
            if key not in catalogue:
                catalogue[key] = []
            catalogue[key].append(row)
    return catalogue


# ── Unique BSB volumes for Weimar period ─────────────────────────────────

def get_weimar_bsb_volumes():
    """Get all unique BSB volume IDs from the existing catalogue."""
    csv_path = os.path.join(SCRIPT_DIR, "weimar_ag_rollcall_master.csv")
    if not os.path.exists(csv_path):
        return []

    volumes = set()
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            volumes.add(row['bsb_volume_id'])
    return sorted(volumes)


# ── MP name parsing from vote list pages ─────────────────────────────────

NOISE_WORDS = {
    'Sie', 'Wort', 'Frage', 'Ende', 'Stunde', 'Rolle', 'Krieg', 'Meinung',
    'Ordnung', 'Kommunen', 'Statistik', 'Gewalt', 'Tagesordnung', 'Deutschland',
    'Zusammenhang', 'Heiterkeit', 'Zurufe', 'Wirtschaftsleben', 'Verfassung',
    'Kriegführung', 'Unterschlupf', 'Schwachheit', 'Steuerzahler', 'Zahlung',
    'Febr', 'Febrpar', 'Meine', 'Damen', 'Herren', 'Abgeordneter', 'Reichstag',
    'Sitzung', 'Präsident', 'Vizepräsident',
}


def is_likely_mp_name(name):
    """Check if a string looks like a plausible MP name."""
    name = name.strip()
    if not name or len(name) < 2:
        return False
    if name in NOISE_WORDS:
        return False
    if any(c in name for c in '!?.;:()'):
        return False
    if '. . .' in name:
        return False  # OCR artifact
    if re.match(r'^\d', name):
        return False
    # Must start with uppercase
    if not name[0].isupper() and not name.startswith('v.') and not name.startswith('von '):
        return False
    return True


def parse_vote_list_page(text):
    """Parse a vote list page to extract MP names and their column votes.
    Returns list of (name, party, vote_columns) tuples."""

    results = []

    # The format is typically:
    # Name  1  (where 1 = Ja in column 1, etc.)
    # or with party section headers like "Sozialdemokratische Partei"

    # Split into lines
    lines = text.split('\n') if '\n' in text else [text]

    # For OCR text that comes as space-separated, try to find name + vote patterns
    # Pattern: Name followed by digits (column votes)
    name_vote_pattern = re.compile(
        r'([A-ZÄÖÜ][a-zäöüß]+(?:\s+(?:von|v\.|Dr\.|Frau|Graf|Frhr?\.|Fhr\.)\s+[A-ZÄÖÜ][a-zäöüß]+)*)'
        r'\s+(\d(?:\s+\d)*)'
    )

    for match in name_vote_pattern.finditer(text):
        name = match.group(1).strip()
        columns = match.group(2).strip().split()

        if is_likely_mp_name(name) and columns:
            results.append({
                'name': name,
                'columns': [int(c) for c in columns if c.isdigit()],
            })

    return results


# ═══════════════════════════════════════════════════════════════════════════
# Main collection workflow
# ═══════════════════════════════════════════════════════════════════════════

def collect_verified_rollcalls(volumes=None, max_volumes=None):
    """Scan BSB volumes for actual Zusammenstellung pages and extract
    verified agricultural roll call vote data.

    Strategy:
    1. Search each volume for tally pages ("Zusammenstellung Abgegebene Stimmzettel")
    2. From each tally page, look backward to find the header with bill list
    3. Parse the header to get date, session, and numbered bills
    4. Filter for agricultural bills
    5. Extract the Zusammenstellung aggregate vote counts
    """

    if volumes is None:
        volumes = get_weimar_bsb_volumes()

    if max_volumes:
        volumes = volumes[:max_volumes]

    existing_catalogue = load_existing_catalogue()

    all_rollcalls = []

    print(f"Scanning {len(volumes)} BSB volumes for roll call vote pages...")
    print(f"Strategy: find tally pages → look backward for bill headers → filter agricultural\n")

    for vol_idx, bsb_id in enumerate(volumes):
        print(f"[{vol_idx+1}/{len(volumes)}] Volume {bsb_id}")

        # Step 1: Find tally (Zusammenstellung) pages
        tally_pages = find_tally_pages(bsb_id)
        time.sleep(API_DELAY)

        if not tally_pages:
            print(f"  No Zusammenstellung pages found")
            continue

        print(f"  Tally pages: {tally_pages}")

        # Step 2: For each tally page, find the header page
        processed_headers = set()  # Avoid processing same header twice

        for tc in tally_pages:
            # Try to read the tally page for aggregate vote counts
            tally_text = bsb_get_ocr(bsb_id, tc)
            time.sleep(API_DELAY)
            zusammen = parse_zusammenstellung(tally_text)

            # Find the header page
            header_canvas, header_text = find_header_from_tally(bsb_id, tc)

            if header_canvas is None:
                # This might be a false positive tally page (debate text mentioning
                # Zusammenstellung) or the header is more than 15 pages back
                continue

            if header_canvas in processed_headers:
                continue
            processed_headers.add(header_canvas)

            # Step 3: Parse the header to get date, session, bills
            header = parse_rollcall_header(header_text)
            if not header:
                print(f"  Canvas {header_canvas}: found header but could not parse")
                continue

            # Step 4: Filter for agricultural bills
            ag_bills = []
            non_ag_bills = []
            for bill in header['bills']:
                if is_agricultural_bill(bill['description']):
                    ag_bills.append(bill)
                else:
                    non_ag_bills.append(bill)

            if not ag_bills:
                print(f"  Canvas {header_canvas} ({header['date']} {header['session']}): "
                      f"{len(header['bills'])} bills, NONE agricultural")
                continue

            print(f"  ✓ Canvas {header_canvas} ({header['date']} {header['session']}): "
                  f"{len(ag_bills)}/{len(header['bills'])} agricultural bills")
            for b in ag_bills:
                drk = f" [Nr. {b['drucksache']}]" if b.get('drucksache') else ""
                print(f"    Col {b['column']}: {b['description'][:70]}{drk}")

            if zusammen:
                print(f"    Zusammenstellung: {zusammen}")

            rollcall = {
                'date': header['date'],
                'session': header['session'],
                'year': header['year'],
                'bsb_id': bsb_id,
                'header_canvas': header_canvas,
                'tally_canvas': tc,
                'all_bills': header['bills'],
                'ag_bills': ag_bills,
                'non_ag_bills': non_ag_bills,
                'zusammenstellung': zusammen,
                'verified': True,
            }
            all_rollcalls.append(rollcall)

            # Match to existing catalogue
            session_key = (header['date'], header['session'])
            cat_bills = existing_catalogue.get(session_key, [])
            if cat_bills:
                print(f"    ↔ Matches {len(cat_bills)} existing catalogue entries")

    return all_rollcalls


def write_verified_csv(rollcalls):
    """Write verified roll call catalogue to CSV."""
    output_path = os.path.join(SCRIPT_DIR, "weimar_ag_rollcall_verified.csv")

    fieldnames = [
        'rollcall_id', 'date', 'year', 'session', 'bsb_volume_id',
        'bsb_url', 'header_canvas', 'bill_column', 'bill_description',
        'bill_drucksache', 'vote_yes', 'vote_no', 'vote_abstain',
        'total_ballots', 'verified', 'source',
    ]

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        rc_id = 0
        for rc in rollcalls:
            for bill in rc['ag_bills']:
                rc_id += 1
                row = {
                    'rollcall_id': rc_id,
                    'date': rc['date'],
                    'year': rc['year'],
                    'session': rc['session'],
                    'bsb_volume_id': rc['bsb_id'],
                    'bsb_url': f"https://www.digitale-sammlungen.de/de/view/{rc['bsb_id']}",
                    'header_canvas': rc['header_canvas'],
                    'bill_column': bill['column'],
                    'bill_description': bill['description'],
                    'bill_drucksache': bill.get('drucksache', ''),
                    'verified': rc['verified'],
                    'source': 'BSB OCR extraction (verified Zusammenstellung)',
                }
                if rc['zusammenstellung']:
                    row.update({
                        'vote_yes': rc['zusammenstellung'].get('vote_yes', ''),
                        'vote_no': rc['zusammenstellung'].get('vote_no', ''),
                        'vote_abstain': rc['zusammenstellung'].get('vote_abstain', ''),
                        'total_ballots': rc['zusammenstellung'].get('total_ballots', ''),
                    })
                writer.writerow(row)

    print(f"\nVerified catalogue written to: {output_path}")
    print(f"  {rc_id} verified agricultural roll call bill entries")
    return output_path


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Collect verified Weimar agricultural roll call votes")
    parser.add_argument('--volumes', nargs='*',
                       help='Specific BSB volume IDs to scan')
    parser.add_argument('--max-volumes', type=int,
                       help='Maximum number of volumes to scan')
    parser.add_argument('--test', action='store_true',
                       help='Test mode: scan only the first 3 volumes')
    args = parser.parse_args()

    if args.test:
        args.max_volumes = 3

    volumes = args.volumes
    rollcalls = collect_verified_rollcalls(
        volumes=volumes,
        max_volumes=args.max_volumes,
    )

    if rollcalls:
        write_verified_csv(rollcalls)

    # Print summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Roll call sessions found: {len(rollcalls)}")
    total_bills = sum(len(rc['ag_bills']) for rc in rollcalls)
    print(f"Agricultural bills in roll calls: {total_bills}")
    with_zusammen = sum(1 for rc in rollcalls if rc['zusammenstellung'])
    print(f"With Zusammenstellung (aggregate counts): {with_zusammen}")


if __name__ == '__main__':
    main()
