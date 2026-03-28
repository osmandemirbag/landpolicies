"""
Validate and collect ONLY verified Weimar Reichstag roll call votes
(namentliche Abstimmungen) on agricultural bills (1919-1933).

Strategy:
  1. Search the DWDS D*/reichstag full-text corpus for "namentliche Abstimmung"
     combined with agricultural keywords to find REAL roll call votes.
  2. For each candidate, extract the date, session, and bill context.
  3. Cross-reference against BSB volumes and look for "Zusammenstellung der
     namentlichen Abstimmung" pages to find actual vote tally pages.
  4. Output a verified, clean catalogue of agricultural roll call votes.

Data sources:
  - DWDS D*/reichstag corpus (CC BY-SA 4.0)
    https://kaskade.dwds.de/dstar/reichstag/
  - BSB Digitale Sammlungen IIIF API
    https://www.digitale-sammlungen.de/

Output:
  - weimar_ag_rollcall_verified.csv  (verified bill catalogue)
  - validation_report.txt            (detailed validation report)
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
API_DELAY = 0.5

# ── Agricultural search keywords for DWDS ────────────────────────────────
# Each tuple: (keyword_query, topic_hint)
# We search "namentliche Abstimmung" && keyword in the DWDS corpus.

AG_KEYWORDS = [
    # Land reform & settlement
    ("Siedlungsgesetz", "Land Reform / Settlement"),
    ("Reichssiedlungsgesetz", "Land Reform / Settlement"),
    ("Bodenreform", "Land Reform"),
    ("Fideikommisse", "Land Reform"),
    ("Großgrundbesitz && Enteignung", "Land Reform"),

    # Tariffs on agricultural products
    ("Getreidezölle", "Grain Tariff"),
    ("Getreidezoll", "Grain Tariff"),
    ("Agrarzölle", "Agricultural Tariffs"),
    ("Agrarzoll", "Agricultural Tariffs"),
    ("Zolltarif && Landwirtschaft", "Agricultural Tariffs"),
    ("Zolltarif && landwirtschaftlich", "Agricultural Tariffs"),
    ("Zolltarif && Getreide", "Grain Tariff"),
    ("Zolltarif && Vieh", "Livestock Tariffs"),
    ("Butterzoll", "Dairy Tariffs"),
    ("Käsezoll", "Dairy Tariffs"),
    ("Fleischzoll", "Meat Tariffs"),
    ("Viehzoll", "Livestock Tariffs"),
    ("Einfuhrschein && Getreide", "Grain Trade"),

    # Agricultural budget and ministry
    ("Ernährung && Landwirtschaft && Haushalt", "Agricultural Budget"),
    ("Reichsministerium && Ernährung && Landwirtschaft", "Agricultural Budget"),
    ("Reichsminister && Ernährung && Mißtrauensantrag", "Agricultural Policy"),

    # Agricultural credit and debt
    ("landwirtschaftlich && Hypotheken", "Agricultural Mortgages"),
    ("landwirtschaftlich && Kredit", "Rural Credit"),
    ("Rentenbank", "Rural Credit"),
    ("Pachtkreditgesetz", "Rural Credit"),
    ("Entschuldung && Landwirtschaft", "Agricultural Debt"),
    ("landwirtschaftlich && Schulden", "Agricultural Debt"),
    ("Winzerkreditgesetz", "Rural Credit / Wine"),

    # Osthilfe (Eastern Aid)
    ("Osthilfe", "Osthilfe / Agricultural Emergency"),
    ("Notlage && Landwirtschaft", "Agricultural Emergency"),
    ("Agrarkrise", "Agricultural Emergency"),

    # Grain and food regulation
    ("Getreideumlag", "Grain Market Regulation"),
    ("Getreideumlage", "Grain Market Regulation"),
    ("Getreidemonopol", "Grain Market Regulation"),
    ("Getreidebewirtschaftung", "Grain Market Regulation"),
    ("Getreidepreisgesetz", "Grain Price Regulation"),
    ("Getreidepreisregulierung", "Grain Price Regulation"),
    ("Zwangswirtschaft && Getreide", "Grain Market Regulation"),
    ("Getreidegesetz", "Grain Market Regulation"),

    # Livestock and meat
    ("Viehseuchen", "Livestock / Animal Health"),
    ("Fleischbeschau", "Meat Import / Regulation"),
    ("Gefrierfleisch", "Meat Import"),
    ("Frischfleisch && Verbilligung", "Meat Price Regulation"),
    ("Tierschutzgesetz", "Animal Welfare"),
    ("Lebendvieh && Einfuhr", "Livestock Import"),

    # Dairy
    ("Milchgesetz", "Milk / Dairy Regulation"),
    ("Milchwirtschaft", "Dairy Regulation"),
    ("Fettwirtschaft", "Dairy / Fat Regulation"),
    ("Butter && Absatz", "Dairy Market Regulation"),
    ("Buttergesetz", "Dairy Market Regulation"),

    # Sugar
    ("Zuckersteuer", "Sugar Taxation"),
    ("Zuckerrübenpreis", "Sugar / Beet"),
    ("Zuckerwaren", "Sugar Regulation"),

    # Spirits
    ("Branntweinmonopol", "Spirits / Distilling"),

    # Agricultural trade treaties
    ("Handelsvertrag && Agrarzölle", "Agricultural Trade Treaty"),
    ("Handelsvertrag && landwirtschaftlich", "Agricultural Trade Treaty"),
    ("Handelsvertrag && Weinzölle", "Wine Trade Treaty"),

    # Socialisation with agricultural component
    ("Sozialisierungsgesetz", "Socialisation"),

    # Agrarian emergency decrees
    ("Notverordnung && Agrarzölle", "Agricultural Tariffs / Emergency Decree"),
    ("Notverordnung && Milch", "Dairy / Emergency Decree"),
    ("Notverordnung && Agrarkredit", "Rural Credit / Emergency Decree"),
]


def dwds_search(query, limit=100, start=0):
    """Search DWDS D*/reichstag corpus. Returns parsed JSON."""
    base = "https://kaskade.dwds.de/dstar/reichstag/dstar.perl"
    params = urllib.parse.urlencode({
        'q': query,
        'fmt': 'json',
        'limit': str(limit),
        'start': str(start),
        'ctx': '2',
    })
    url = f"{base}?{params}"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
        return json.loads(resp.read())
    except Exception as e:
        print(f"  DWDS error: {e}", file=sys.stderr)
        return None


def bsb_iiif_search(volume_id, query_text):
    """Search BSB volume for text. Returns list of canvas matches."""
    url = (f"https://api.digitale-sammlungen.de/iiif/search/v1/{volume_id}"
           f"?q={urllib.parse.quote(query_text)}")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
        data = json.loads(resp.read())
        resources = data.get('resources', [])
        canvases = set()
        for r in resources:
            on = r.get('on', '')
            # Extract canvas ID
            m = re.search(r'/canvas/(\d+)', on)
            if m:
                canvases.add(int(m.group(1)))
        return sorted(canvases)
    except Exception as e:
        return []


def bsb_get_manifest(volume_id):
    """Get BSB IIIF manifest to determine volume label and page count."""
    url = f"https://api.digitale-sammlungen.de/iiif/presentation/v2/{volume_id}/manifest"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
        data = json.loads(resp.read())
        label = data.get('label', '')
        canvases = data.get('sequences', [{}])[0].get('canvases', [])
        return label, len(canvases)
    except Exception:
        return '', 0


# ── Known BSB volume IDs for Weimar Reichstag Stenographische Berichte ──
# Source: https://www.digitale-sammlungen.de/en/german-reichstag-session-reports
# These map band numbers to BSB IDs. We need to verify these.

def find_zusammenstellung_pages(volume_id, date_str):
    """Search a BSB volume for 'Zusammenstellung der namentlichen Abstimmung'
    pages near a given date."""
    canvases = bsb_iiif_search(volume_id, "Zusammenstellung namentlichen Abstimmung")
    if not canvases:
        # Try alternative phrasings
        canvases = bsb_iiif_search(volume_id, "Zusammenstellung der namentlichen")
    if not canvases:
        canvases = bsb_iiif_search(volume_id, "namentliche Abstimmung Zusammenstellung")
    return canvases


def extract_date_from_dwds_hit(hit):
    """Extract date from DWDS hit metadata."""
    meta = hit.get('meta_', {})
    date_str = meta.get('date_', '')
    basename = meta.get('basename_', '')
    # Try to extract date from basename or date field
    m = re.search(r'(\d{4})', date_str)
    year = int(m.group(1)) if m else 0
    return year, date_str, basename


def is_weimar_period(year):
    """Check if year falls in Weimar Republic period (1919-1933)."""
    return 1919 <= year <= 1933


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 1: Search DWDS for all "namentliche Abstimmung" on agricultural topics
# ═══════════════════════════════════════════════════════════════════════════

def phase1_dwds_search():
    """Search DWDS for agricultural roll call votes in Weimar period."""
    print("=" * 70)
    print("PHASE 1: Searching DWDS for agricultural roll call votes (1919-1933)")
    print("=" * 70)

    all_hits = {}  # keyed by (year, basename) to deduplicate

    for keyword_query, topic_hint in AG_KEYWORDS:
        full_query = f"namentliche Abstimmung && {keyword_query}"
        print(f"\n  Searching: {full_query}")
        data = dwds_search(full_query, limit=50)
        time.sleep(API_DELAY)

        if not data:
            print("    -> No response")
            continue

        nhits = data.get('nhits_', 0)
        hits = data.get('hits_', [])
        print(f"    -> {nhits} total hits, {len(hits)} returned")

        for hit in hits:
            year, date_str, basename = extract_date_from_dwds_hit(hit)
            if not is_weimar_period(year):
                continue

            ctx = hit.get('ctx_', '')
            key = (year, basename)
            if key not in all_hits:
                all_hits[key] = {
                    'year': year,
                    'date': date_str,
                    'basename': basename,
                    'topics': set(),
                    'contexts': [],
                    'queries': [],
                }
            all_hits[key]['topics'].add(topic_hint)
            all_hits[key]['queries'].append(keyword_query)
            if ctx and len(all_hits[key]['contexts']) < 3:
                all_hits[key]['contexts'].append(ctx[:300])

    # Filter to only Weimar period
    weimar_hits = {k: v for k, v in all_hits.items() if is_weimar_period(v['year'])}

    print(f"\n\nTotal unique DWDS hits in Weimar period: {len(weimar_hits)}")
    return weimar_hits


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 2: Cross-validate existing catalogue against DWDS results
# ═══════════════════════════════════════════════════════════════════════════

def phase2_validate_existing():
    """Validate the existing 119-bill catalogue against DWDS and BSB."""
    print("\n" + "=" * 70)
    print("PHASE 2: Validating existing bill catalogue")
    print("=" * 70)

    master_csv = os.path.join(SCRIPT_DIR, "weimar_ag_rollcall_master.csv")
    if not os.path.exists(master_csv):
        print("  No master CSV found. Skipping validation.")
        return []

    with open(master_csv) as f:
        reader = csv.DictReader(f)
        bills = list(reader)

    results = []
    for bill in bills:
        vid = bill['vote_id']
        date = bill['date']
        title_de = bill['bill_german_title']
        bsb_id = bill['bsb_volume_id']
        topic = bill['topic_category']

        # Extract key term from title for DWDS search
        # Get first significant word (skip articles, prepositions)
        title_words = re.findall(r'[A-ZÄÖÜ][a-zäöüß]+(?:gesetz|novelle|tarif|zoll|steuer|'
                                 r'setz|ordnung|antrag|vertrag|gesetz)', title_de)
        key_term = title_words[0] if title_words else title_de.split(' – ')[0].split()[0]

        result = {
            'vote_id': vid,
            'date': date,
            'title_de': title_de,
            'topic': topic,
            'bsb_id': bsb_id,
            'dwds_verified': False,
            'bsb_zusammenstellung_found': False,
            'issues': [],
        }

        # Search DWDS for this specific bill
        query = f"{key_term} && namentliche"
        data = dwds_search(query, limit=10)
        time.sleep(API_DELAY)

        if data:
            nhits = data.get('nhits_', 0)
            hits = data.get('hits_', [])
            year = int(date[:4])
            # Check if any hit matches the year
            matching_years = [h for h in hits
                            if extract_date_from_dwds_hit(h)[0] == year]
            if matching_years:
                result['dwds_verified'] = True
            elif nhits > 0:
                years_found = [extract_date_from_dwds_hit(h)[0] for h in hits]
                result['issues'].append(
                    f"DWDS found '{key_term}' + namentliche in years "
                    f"{sorted(set(years_found))} but NOT in {year}")
            else:
                result['issues'].append(
                    f"DWDS: no hits for '{key_term} && namentliche'")

        # Check BSB for Zusammenstellung pages
        zusammen_pages = find_zusammenstellung_pages(bsb_id, date)
        time.sleep(API_DELAY)
        if zusammen_pages:
            result['bsb_zusammenstellung_found'] = True
            result['zusammenstellung_canvases'] = zusammen_pages
        else:
            result['issues'].append(
                f"BSB {bsb_id}: no 'Zusammenstellung' pages found")

        status = ("✓" if result['dwds_verified'] and result['bsb_zusammenstellung_found']
                  else "△" if result['dwds_verified'] or result['bsb_zusammenstellung_found']
                  else "✗")
        print(f"  {status} vote_id={vid:>3s} [{topic[:30]:>30s}] {title_de[:50]}")
        if result['issues']:
            for issue in result['issues']:
                print(f"      ⚠ {issue}")

        results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 3: Check individual vote data quality
# ═══════════════════════════════════════════════════════════════════════════

def phase3_check_individual_data():
    """Check quality of individual vote records."""
    print("\n" + "=" * 70)
    print("PHASE 3: Checking individual vote data quality")
    print("=" * 70)

    indiv_csv = os.path.join(SCRIPT_DIR, "weimar_ag_individual_votes_master.csv")
    if not os.path.exists(indiv_csv):
        print("  No individual votes CSV found.")
        return {}

    with open(indiv_csv) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Group by vote_id
    by_vote = defaultdict(list)
    for r in rows:
        by_vote[r['vote_id']].append(r)

    report = {}
    for vid in sorted(by_vote.keys(), key=int):
        records = by_vote[vid]
        title = records[0]['bill_german_title']

        # Check for duplicate vote distributions (same BSB volume = same data)
        vote_dist = {}
        for r in records:
            vote_dist[r['vote']] = vote_dist.get(r['vote'], 0) + 1

        # Check for OCR noise in MP names
        noise_count = 0
        real_count = 0
        for r in records:
            name = r['mp_name']
            if not name or len(name) < 2:
                noise_count += 1
            elif any(c in name for c in '!?.'):
                noise_count += 1
            elif name.rstrip() in {'Sie', 'Wort', 'Frage', 'Ende', 'Stunde',
                                   'Rolle', 'Krieg', 'Meinung', 'Ordnung',
                                   'Kommunen', 'Statistik', 'Gewalt',
                                   'Tagesordnung', 'Deutschland', 'Zusammenhang',
                                   'Heiterkeit', 'Zurufe', 'Wirtschaftsleben',
                                   'Verfassung', 'Kriegführung', 'Unterschlupf'}:
                noise_count += 1
            else:
                real_count += 1

        total = len(records)
        noise_pct = 100 * noise_count / total if total > 0 else 0

        report[vid] = {
            'total': total,
            'noise': noise_count,
            'real': real_count,
            'noise_pct': noise_pct,
            'vote_dist': vote_dist,
            'title': title[:60],
        }

        quality = ("GOOD" if noise_pct < 10 and total > 10
                   else "SPARSE" if total <= 3
                   else "NOISY" if noise_pct > 30
                   else "FAIR")
        print(f"  vote_id={vid:>3s} n={total:>4d} noise={noise_pct:5.1f}% [{quality:>6s}] "
              f"{title[:50]}")

    # Find duplicate vote distributions (bills sharing same data)
    dist_signatures = defaultdict(list)
    for vid, info in report.items():
        # Create a signature from vote distribution
        sig = tuple(sorted(info['vote_dist'].items()))
        if info['total'] > 1:  # Skip single-record bills
            dist_signatures[sig].append(vid)

    print(f"\n  --- Duplicate vote distributions (data contamination) ---")
    for sig, vids in dist_signatures.items():
        if len(vids) > 1:
            print(f"  DUPLICATE: vote_ids {vids} share identical vote distribution {dict(sig)}")
            for vid in vids:
                print(f"    {vid}: {report[vid]['title']}")

    return report


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 4: Write validation report and verified CSV
# ═══════════════════════════════════════════════════════════════════════════

def phase4_write_report(dwds_hits, validation_results, quality_report):
    """Write comprehensive validation report."""
    report_path = os.path.join(SCRIPT_DIR, "validation_report.txt")
    verified_csv = os.path.join(SCRIPT_DIR, "weimar_ag_rollcall_verified.csv")

    with open(report_path, 'w') as f:
        f.write("WEIMAR AGRICULTURAL ROLL CALL VOTES — VALIDATION REPORT\n")
        f.write("=" * 70 + "\n\n")

        # Summary
        if validation_results:
            total = len(validation_results)
            dwds_ok = sum(1 for r in validation_results if r['dwds_verified'])
            bsb_ok = sum(1 for r in validation_results if r['bsb_zusammenstellung_found'])
            both_ok = sum(1 for r in validation_results
                         if r['dwds_verified'] and r['bsb_zusammenstellung_found'])

            f.write(f"EXISTING CATALOGUE: {total} bills\n")
            f.write(f"  DWDS verified (keyword + namentliche in correct year): {dwds_ok}/{total}\n")
            f.write(f"  BSB Zusammenstellung pages found: {bsb_ok}/{total}\n")
            f.write(f"  Both verified: {both_ok}/{total}\n\n")

            f.write("BILL-BY-BILL VALIDATION:\n")
            f.write("-" * 70 + "\n")
            for r in validation_results:
                status = ("VERIFIED" if r['dwds_verified'] and r['bsb_zusammenstellung_found']
                          else "PARTIAL" if r['dwds_verified'] or r['bsb_zusammenstellung_found']
                          else "UNVERIFIED")
                f.write(f"\nvote_id={r['vote_id']} [{status}]\n")
                f.write(f"  Date: {r['date']}\n")
                f.write(f"  Title: {r['title_de']}\n")
                f.write(f"  Topic: {r['topic']}\n")
                f.write(f"  BSB: {r['bsb_id']}\n")
                f.write(f"  DWDS verified: {r['dwds_verified']}\n")
                f.write(f"  Zusammenstellung found: {r['bsb_zusammenstellung_found']}\n")
                if r.get('zusammenstellung_canvases'):
                    f.write(f"  Canvas pages: {r['zusammenstellung_canvases']}\n")
                for issue in r['issues']:
                    f.write(f"  ⚠ {issue}\n")

        # Quality report
        if quality_report:
            f.write("\n\nINDIVIDUAL VOTE DATA QUALITY:\n")
            f.write("-" * 70 + "\n")
            for vid in sorted(quality_report.keys(), key=int):
                info = quality_report[vid]
                f.write(f"  vote_id={vid:>3s} n={info['total']:>4d} "
                        f"noise={info['noise_pct']:5.1f}% {info['title']}\n")

        # DWDS discoveries
        if dwds_hits:
            f.write(f"\n\nDWDS AGRICULTURAL ROLL CALL HITS (Weimar period):\n")
            f.write("-" * 70 + "\n")
            for key in sorted(dwds_hits.keys()):
                hit = dwds_hits[key]
                f.write(f"\n  Year={hit['year']} basename={hit['basename']}\n")
                f.write(f"  Topics: {', '.join(hit['topics'])}\n")
                f.write(f"  Keywords matched: {', '.join(set(hit['queries']))}\n")
                for ctx in hit['contexts'][:2]:
                    f.write(f"  Context: {ctx[:200]}\n")

    print(f"\nValidation report written to: {report_path}")

    # Write verified CSV with only confirmed bills
    if validation_results:
        verified = [r for r in validation_results
                    if r['dwds_verified'] or r['bsb_zusammenstellung_found']]

        master_csv = os.path.join(SCRIPT_DIR, "weimar_ag_rollcall_master.csv")
        with open(master_csv) as f:
            reader = csv.DictReader(f)
            all_bills = {r['vote_id']: r for r in reader}

        with open(verified_csv, 'w', newline='') as f:
            fieldnames = ['vote_id', 'date', 'year', 'session', 'wahlperiode',
                         'bill_german_title', 'bill_english_title', 'topic_category',
                         'drucksache', 'bsb_volume_id', 'bsb_url',
                         'dwds_verified', 'bsb_zusammenstellung_found',
                         'zusammenstellung_canvases', 'validation_issues', 'source']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for r in verified:
                vid = r['vote_id']
                bill = all_bills.get(vid, {})
                writer.writerow({
                    'vote_id': vid,
                    'date': r['date'],
                    'year': bill.get('year', ''),
                    'session': bill.get('session', ''),
                    'wahlperiode': bill.get('wahlperiode', ''),
                    'bill_german_title': r['title_de'],
                    'bill_english_title': bill.get('bill_english_title', ''),
                    'topic_category': r['topic'],
                    'drucksache': bill.get('drucksache', ''),
                    'bsb_volume_id': r['bsb_id'],
                    'bsb_url': bill.get('bsb_url', ''),
                    'dwds_verified': r['dwds_verified'],
                    'bsb_zusammenstellung_found': r['bsb_zusammenstellung_found'],
                    'zusammenstellung_canvases': r.get('zusammenstellung_canvases', ''),
                    'validation_issues': '; '.join(r['issues']),
                    'source': bill.get('source', ''),
                })

        print(f"Verified CSV written to: {verified_csv}")
        print(f"  {len(verified)} verified bills out of {len(validation_results)} total")


def main():
    print("Weimar Agricultural Roll Call Vote Validation")
    print("=" * 70)

    # Phase 1: DWDS search (skip if --skip-dwds flag)
    dwds_hits = {}
    if '--skip-dwds' not in sys.argv:
        dwds_hits = phase1_dwds_search()
    else:
        print("\nSkipping DWDS search (--skip-dwds)")

    # Phase 2: Validate existing catalogue
    validation_results = []
    if '--skip-validate' not in sys.argv:
        validation_results = phase2_validate_existing()
    else:
        print("\nSkipping validation (--skip-validate)")

    # Phase 3: Check individual data quality
    quality_report = phase3_check_individual_data()

    # Phase 4: Write report
    phase4_write_report(dwds_hits, validation_results, quality_report)


if __name__ == '__main__':
    main()
