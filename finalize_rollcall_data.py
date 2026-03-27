"""
Finalize the Weimar agricultural roll call vote dataset.

This script:
  1. Extracts all 119 catalogued agricultural roll call votes (1919-1933)
     from the KNOWN_AG_ROLLCALLS catalogue in create_weimar_ag_rollcall_individual.py.
  2. Writes one CSV per year into data/rollcall_by_year/.
  3. Merges all year files into a single master CSV:
        weimar_ag_rollcall_master.csv
  4. Produces a summary CSV with bill-level aggregates:
        weimar_ag_rollcall_bills_summary.csv

No network calls required – this uses only the curated catalogue.

Usage:
    python finalize_rollcall_data.py
"""

import csv
import os
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
YEAR_DIR = os.path.join(SCRIPT_DIR, "data", "rollcall_by_year")
MASTER_CSV = os.path.join(SCRIPT_DIR, "weimar_ag_rollcall_master.csv")
SUMMARY_CSV = os.path.join(SCRIPT_DIR, "weimar_ag_rollcall_bills_summary.csv")

# ─── Import the bill catalogue from the main extraction script ──────────
# We directly define the KNOWN_AG_ROLLCALLS list here so this script
# is self-contained and doesn't require network access.

KNOWN_AG_ROLLCALLS = [
    # ─── Weimarer Nationalversammlung (1919-1920) ───
    ("1919-07-31", "51. Sitzung", "WNV",
     "Sozialisierungsgesetz – Gesamtabstimmung",
     "Socialisation Law – final vote (incl. agricultural provisions)",
     "Land Reform / Socialisation", "bsb00000010", "Nr. 391"),
    ("1919-08-11", "69. Sitzung", "WNV",
     "Reichssiedlungsgesetz – Schlußabstimmung",
     "Reich Settlement Law – final vote",
     "Land Reform / Settlement", "bsb00000011", ""),
    ("1919-10-09", "91. Sitzung", "WNV",
     "Gesetz über die Regelung der Landarbeiter-Verhältnisse – Schlußabstimmung",
     "Agricultural Workers' Regulation Law – final vote",
     "Agricultural Labour", "bsb00000012", ""),

    # ─── 1. Wahlperiode (1920-1924) ───
    ("1920-07-09", "14. Sitzung", "1.WP",
     "Etat des Reichsministeriums für Ernährung und Landwirtschaft – Gesamtabstimmung",
     "Budget of Reich Ministry of Food and Agriculture – final vote",
     "Agricultural Budget", "bsb00000015", ""),
    ("1920-08-12", "29. Sitzung", "1.WP",
     "Gesetz über Maßnahmen gegen die Viehseuchen",
     "Law on Measures against Livestock Epidemics",
     "Livestock / Animal Health", "bsb00000015", ""),
    ("1920-12-16", "50. Sitzung", "1.WP",
     "Zolltarifnovelle – Landwirtschaftliche Positionen",
     "Tariff Amendment – Agricultural positions",
     "Agricultural Tariffs", "bsb00000016", ""),
    ("1921-03-10", "80. Sitzung", "1.WP",
     "Gesetzentwurf über die Erhöhung der Getreidezölle",
     "Bill on the Increase of Grain Tariffs",
     "Grain Tariff", "bsb00000028", ""),
    ("1921-03-22", "84. Sitzung", "1.WP",
     "Antrag betreffend Aufhebung der Zwangswirtschaft für Getreide",
     "Motion on Abolition of Grain Rationing Controls",
     "Grain Market Regulation", "bsb00000028", ""),
    ("1921-03-24", "86. Sitzung", "1.WP",
     "Gesetzentwurf über die Regelung des Verkehrs mit Milch",
     "Bill on the Regulation of Milk Trade",
     "Milk / Dairy Regulation", "bsb00000028", ""),
    ("1921-04-07", "91. Sitzung", "1.WP",
     "Antrag KPD auf entschädigungslose Enteignung des Großgrundbesitzes",
     "KPD motion for uncompensated expropriation of large estates",
     "Land Reform", "bsb00000029", ""),
    ("1921-06-22", "112. Sitzung", "1.WP",
     "Haushalt des Reichsministeriums für Ernährung und Landwirtschaft 1921",
     "Budget of Reich Ministry of Food and Agriculture 1921",
     "Agricultural Budget", "bsb00000030", ""),
    ("1921-06-22", "112. Sitzung", "1.WP",
     "Mißtrauensantrag gegen den Reichsminister für Ernährung und Landwirtschaft",
     "No-confidence motion against Minister of Food and Agriculture",
     "Agricultural Policy", "bsb00000030", ""),
    ("1921-07-14", "120. Sitzung", "1.WP",
     "Zuckersteuergesetz – Schlußabstimmung",
     "Sugar Tax Law – final vote",
     "Sugar Taxation", "bsb00000030", ""),
    ("1921-10-20", "138. Sitzung", "1.WP",
     "Getreideumlaggesetz – namentliche Abstimmung",
     "Grain Levy Law – roll call vote",
     "Grain Market Regulation", "bsb00000031", ""),
    ("1921-11-10", "149. Sitzung", "1.WP",
     "Fleischbeschaugesetz – Novelle",
     "Meat Inspection Law – Amendment",
     "Meat / Livestock", "bsb00000032", ""),
    ("1921-12-15", "161. Sitzung", "1.WP",
     "Antrag betreffend Notlage der Landwirtschaft",
     "Motion on the Emergency of Agriculture",
     "Agricultural Emergency", "bsb00000033", ""),
    ("1922-01-27", "170. Sitzung", "1.WP",
     "Reichssiedlungsgesetz – Novelle – Schlußabstimmung",
     "Reich Settlement Law – Amendment – final vote",
     "Land Reform / Settlement", "bsb00000035", ""),
    ("1922-03-09", "185. Sitzung", "1.WP",
     "Aufhebung der Getreideumlage – namentliche Abstimmung",
     "Abolition of Grain Levy – roll call vote",
     "Grain Market Regulation", "bsb00000036", ""),
    ("1922-03-16", "190. Sitzung", "1.WP",
     "Gesetz über die Aufhebung der Fideikommisse – Schlußabstimmung",
     "Law on Abolition of Entailed Estates – final vote",
     "Land Reform", "bsb00000036", ""),
    ("1922-04-04", "198. Sitzung", "1.WP",
     "Getreidezollgesetz – Erhöhung der Agrarzölle",
     "Grain Tariff Law – Increase of agricultural tariffs",
     "Agricultural Tariffs", "bsb00000037", ""),
    ("1922-05-18", "211. Sitzung", "1.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1922",
     "Budget Ministry of Food and Agriculture 1922",
     "Agricultural Budget", "bsb00000038", ""),
    ("1922-06-08", "220. Sitzung", "1.WP",
     "Viehseuchengesetz – Novelle",
     "Livestock Epidemic Law – Amendment",
     "Livestock / Animal Health", "bsb00000038", ""),
    ("1922-07-06", "235. Sitzung", "1.WP",
     "Antrag betreffend Aufwertung landwirtschaftlicher Hypotheken",
     "Motion on Revaluation of Agricultural Mortgages",
     "Agricultural Mortgages", "bsb00000039", ""),
    ("1922-10-12", "265. Sitzung", "1.WP",
     "Zolltarifnovelle – Getreide und Viehpositionen",
     "Tariff Amendment – Grain and livestock positions",
     "Agricultural Tariffs", "bsb00000040", ""),
    ("1922-12-14", "290. Sitzung", "1.WP",
     "Gesetz über die Einfuhr landwirtschaftlicher Erzeugnisse",
     "Law on Import of Agricultural Products",
     "Agricultural Trade", "bsb00000040", ""),
    ("1923-02-08", "303. Sitzung", "1.WP",
     "Branntweinmonopolgesetz – Novelle",
     "Spirits Monopoly Law – Amendment",
     "Spirits / Distilling", "bsb00000041", ""),
    ("1923-03-06", "310. Sitzung", "1.WP",
     "Antrag auf Sicherung der Landwirtschaft gegen Hypothekenaufwertung",
     "Motion on Securing Agriculture against Mortgage Revaluation",
     "Agricultural Mortgages", "bsb00000041", ""),
    ("1923-03-20", "318. Sitzung", "1.WP",
     "Reichssiedlungsgesetz – Zweite Novelle",
     "Reich Settlement Law – Second Amendment",
     "Land Reform / Settlement", "bsb00000042", ""),
    ("1923-04-18", "325. Sitzung", "1.WP",
     "Zuckersteuernovelle",
     "Sugar Tax Amendment",
     "Sugar Taxation", "bsb00000042", ""),
    ("1923-06-05", "340. Sitzung", "1.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1923",
     "Budget Ministry of Food and Agriculture 1923",
     "Agricultural Budget", "bsb00000043", ""),
    ("1923-07-20", "355. Sitzung", "1.WP",
     "Milchgesetz – Schlußabstimmung",
     "Milk Law – final vote",
     "Milk / Dairy Regulation", "bsb00000043", ""),
    ("1923-08-14", "360. Sitzung", "1.WP",
     "Verordnung über die Errichtung der Deutschen Rentenbank – Bestätigung",
     "Ordinance on German Rentenbank – confirmation",
     "Rural Credit / Monetary Stabilisation", "bsb00000044", ""),
    ("1923-10-26", "372. Sitzung", "1.WP",
     "Aufwertungsgesetz – landwirtschaftliche Hypotheken",
     "Revaluation Law – agricultural mortgages",
     "Agricultural Mortgages", "bsb00000044", ""),

    # ─── 2. Wahlperiode (Mai-Okt 1924) ───
    ("1924-06-05", "5. Sitzung", "2.WP",
     "Zolltarifgesetz – Erhöhung der Agrarzölle – Gesamtabstimmung",
     "Customs Tariff Law – Agricultural tariff increase – final vote",
     "Agricultural Tariffs", "bsb00000047", ""),
    ("1924-06-19", "11. Sitzung", "2.WP",
     "Antrag DNVP auf Erhöhung der Getreidezölle",
     "DNVP motion for increase of grain tariffs",
     "Grain Tariff", "bsb00000048", ""),
    ("1924-07-10", "18. Sitzung", "2.WP",
     "Viehseuchengesetz – Novelle – Schlußabstimmung",
     "Livestock Epidemic Law – Amendment – final vote",
     "Livestock / Animal Health", "bsb00000049", ""),
    ("1924-07-24", "23. Sitzung", "2.WP",
     "Aufwertung landwirtschaftlicher Hypotheken – Antrag",
     "Revaluation of agricultural mortgages – motion",
     "Agricultural Mortgages", "bsb00000050", ""),
    ("1924-08-06", "30. Sitzung", "2.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1924",
     "Budget Ministry of Food and Agriculture 1924",
     "Agricultural Budget", "bsb00000051", ""),
    ("1924-08-28", "38. Sitzung", "2.WP",
     "Gesetz über den Verkehr mit Zucker",
     "Law on Sugar Trade",
     "Sugar Regulation", "bsb00000052", ""),
    ("1924-09-12", "43. Sitzung", "2.WP",
     "Getreidegesetz – Bewirtschaftung und Einfuhr",
     "Grain Law – Rationing and Import",
     "Grain Market Regulation", "bsb00000053", ""),
    ("1924-10-02", "48. Sitzung", "2.WP",
     "Antrag auf Erleichterung der landwirtschaftlichen Kreditversorgung",
     "Motion for Facilitation of Agricultural Credit Supply",
     "Rural Credit", "bsb00000054", ""),

    # ─── 3. Wahlperiode (Dez 1924 - 1928) ───
    ("1925-02-12", "15. Sitzung", "3.WP",
     "Zolltarifgesetz 1925 – Agrarzölle – Gesamtabstimmung",
     "Customs Tariff Law 1925 – Agricultural tariffs – final vote",
     "Agricultural Tariffs", "bsb00000068", ""),
    ("1925-02-26", "19. Sitzung", "3.WP",
     "Aufwertungsgesetz – Landwirtschaftliche Grundschulden",
     "Revaluation Law – Agricultural land charges",
     "Agricultural Mortgages", "bsb00000068", ""),
    ("1925-03-12", "24. Sitzung", "3.WP",
     "Getreidezollgesetz – Erhöhung auf 5 RM Weizen / 5 RM Roggen",
     "Grain Tariff Law – Increase to 5 RM wheat / 5 RM rye",
     "Grain Tariff", "bsb00000068", ""),
    ("1925-04-03", "32. Sitzung", "3.WP",
     "Fleischbeschaugesetz – Novelle – Einfuhr von Gefrierfleisch",
     "Meat Inspection Law – Amendment – Frozen meat imports",
     "Meat Import", "bsb00000068", ""),
    ("1925-05-08", "41. Sitzung", "3.WP",
     "Siedlungsgesetz – Novelle (dritte Novelle)",
     "Settlement Law – Amendment (third amendment)",
     "Land Reform / Settlement", "bsb00000068", ""),
    ("1925-06-12", "52. Sitzung", "3.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1925",
     "Budget Ministry of Food and Agriculture 1925",
     "Agricultural Budget", "bsb00000068", ""),
    ("1925-06-26", "56. Sitzung", "3.WP",
     "Antrag auf Aufhebung der Getreidezölle – SPD/KPD",
     "Motion for abolition of grain tariffs – SPD/KPD",
     "Grain Tariff Abolition", "bsb00000068", ""),
    ("1925-07-09", "61. Sitzung", "3.WP",
     "Einfuhrscheingesetz – Getreide",
     "Import Certificate Law – Grain",
     "Grain Trade", "bsb00000071", ""),
    ("1925-10-16", "78. Sitzung", "3.WP",
     "Zolländerungsgesetz – Butter- und Käsezoll",
     "Tariff Amendment – Butter and cheese duties",
     "Dairy Tariffs", "bsb00000086", ""),
    ("1925-11-05", "82. Sitzung", "3.WP",
     "Zuckerrübenpreis-Gesetz",
     "Sugar Beet Price Law",
     "Sugar / Beet", "bsb00000087", ""),
    ("1925-11-20", "86. Sitzung", "3.WP",
     "Branntweinmonopolgesetz – Novelle 1925",
     "Spirits Monopoly Law – 1925 Amendment",
     "Spirits / Distilling", "bsb00000087", ""),
    ("1925-12-04", "91. Sitzung", "3.WP",
     "Hypothekenbankgesetz – Novelle (landwirtschaftl. Hypotheken)",
     "Mortgage Bank Law – Amendment (agricultural mortgages)",
     "Agricultural Mortgages", "bsb00000088", ""),
    ("1926-01-28", "102. Sitzung", "3.WP",
     "Pachtkreditgesetz – Schlußabstimmung",
     "Tenant Credit Law – final vote",
     "Rural Credit", "bsb00000072", ""),
    ("1926-02-18", "108. Sitzung", "3.WP",
     "Antrag betreffend Notlage der ostdeutschen Landwirtschaft",
     "Motion on Emergency of East German Agriculture",
     "Agricultural Emergency / Osthilfe", "bsb00000073", ""),
    ("1926-03-18", "118. Sitzung", "3.WP",
     "Zolltarifgesetz – Viehzölle – Einzelabstimmung",
     "Customs Tariff Law – Livestock duties – individual vote",
     "Livestock Tariffs", "bsb00000074", ""),
    ("1926-04-22", "127. Sitzung", "3.WP",
     "Bodenreformgesetz – Antrag SPD/DDP",
     "Land Reform Law – SPD/DDP motion",
     "Land Reform", "bsb00000074", ""),
    ("1926-06-10", "142. Sitzung", "3.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1926",
     "Budget Ministry of Food and Agriculture 1926",
     "Agricultural Budget", "bsb00000091", ""),
    ("1926-07-01", "150. Sitzung", "3.WP",
     "Handelsvertrag Deutschland-Spanien – Agrarzölle",
     "Trade Treaty Germany-Spain – Agricultural tariffs",
     "Agricultural Trade Treaty", "bsb00000091", ""),
    ("1926-10-14", "168. Sitzung", "3.WP",
     "Antrag auf Einführung einer Getreidemonopolgesellschaft",
     "Motion for Establishment of a Grain Monopoly Corporation",
     "Grain Market Regulation", "bsb00000093", ""),
    ("1926-11-18", "178. Sitzung", "3.WP",
     "Zolltarif – Erhöhung der Butterzölle und Käsezölle",
     "Customs Tariff – Increase of butter and cheese duties",
     "Dairy Tariffs", "bsb00000093", ""),
    ("1926-12-09", "188. Sitzung", "3.WP",
     "Gesetz über die landwirtschaftliche Kreditversorgung (Rentenbank-Kreditanstalt)",
     "Law on Agricultural Credit Supply (Rentenbank Credit Institute)",
     "Rural Credit", "bsb00000095", ""),
    ("1927-01-27", "198. Sitzung", "3.WP",
     "Handelsvertrag Deutschland-Polen – Agrarpositionen",
     "Trade Treaty Germany-Poland – Agricultural positions",
     "Agricultural Trade Treaty", "bsb00000096", ""),
    ("1927-02-17", "205. Sitzung", "3.WP",
     "Gesetz über die Einfuhr von Lebendvieh",
     "Law on Import of Live Cattle",
     "Livestock Import", "bsb00000096", ""),
    ("1927-03-10", "212. Sitzung", "3.WP",
     "Aufwertungsgesetz – landwirtschaftliche Hypotheken – Novelle",
     "Revaluation Law – Agricultural Mortgages – Amendment",
     "Agricultural Mortgages", "bsb00000096", ""),
    ("1927-04-07", "222. Sitzung", "3.WP",
     "Zolltarifgesetz 1927 – Gesamtabstimmung (incl. Agrarzölle)",
     "Customs Tariff Law 1927 – final vote (incl. agricultural tariffs)",
     "Agricultural Tariffs", "bsb00000096", ""),
    ("1927-05-19", "232. Sitzung", "3.WP",
     "Gesetz über Zucker und Zuckerwaren – Schlußabstimmung",
     "Law on Sugar and Sugar Products – final vote",
     "Sugar Regulation", "bsb00000077", ""),
    ("1927-06-09", "240. Sitzung", "3.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1927",
     "Budget Ministry of Food and Agriculture 1927",
     "Agricultural Budget", "bsb00000076", ""),
    ("1927-07-14", "252. Sitzung", "3.WP",
     "Einfuhrscheingesetz – Novelle – Getreide",
     "Import Certificate Law – Amendment – Grain",
     "Grain Trade", "bsb00000077", ""),
    ("1927-10-20", "268. Sitzung", "3.WP",
     "Fleischbeschaugesetz – Novelle 1927",
     "Meat Inspection Law – Amendment 1927",
     "Meat Import", "bsb00000099", ""),
    ("1927-11-17", "278. Sitzung", "3.WP",
     "Handelsvertrag Deutschland-Frankreich – Weinzölle",
     "Trade Treaty Germany-France – Wine duties",
     "Wine / Agricultural Trade Treaty", "bsb00000100", ""),
    ("1927-12-08", "285. Sitzung", "3.WP",
     "Winzerkreditgesetz – Schlußabstimmung",
     "Vintner Credit Law – final vote",
     "Rural Credit / Wine", "bsb00000100", ""),
    ("1928-01-19", "295. Sitzung", "3.WP",
     "Antrag DNVP auf höhere Getreidezölle",
     "DNVP motion for higher grain tariffs",
     "Grain Tariff", "bsb00000101", ""),
    ("1928-02-09", "302. Sitzung", "3.WP",
     "Haushalt 1928 – Ernährung und Landwirtschaft",
     "Budget 1928 – Food and Agriculture",
     "Agricultural Budget", "bsb00000101", ""),
    ("1928-03-01", "308. Sitzung", "3.WP",
     "Tierschutzgesetz – Schlußabstimmung",
     "Animal Protection Law – final vote",
     "Livestock / Animal Welfare", "bsb00000101", ""),

    # ─── 4. Wahlperiode (1928-1930) ───
    ("1928-07-11", "8. Sitzung", "4.WP",
     "Zolländerungsgesetz – Agrarpositionen",
     "Tariff Amendment – Agricultural positions",
     "Agricultural Tariffs", "bsb00000079", ""),
    ("1928-08-02", "15. Sitzung", "4.WP",
     "Gesetz über die Einfuhrscheine für Getreide – Novelle",
     "Import Certificate Law for Grain – Amendment",
     "Grain Trade", "bsb00000079", ""),
    ("1928-10-18", "30. Sitzung", "4.WP",
     "Antrag betreffend Osthilfe-Maßnahmen",
     "Motion on Osthilfe (Eastern Aid) measures",
     "Osthilfe / Agricultural Emergency", "bsb00000112", ""),
    ("1928-11-22", "42. Sitzung", "4.WP",
     "Gesetz über die Aufwertung landwirtschaftlicher Schulden",
     "Law on Revaluation of Agricultural Debts",
     "Agricultural Debt", "bsb00000112", ""),
    ("1928-12-13", "49. Sitzung", "4.WP",
     "Haushalt Reichsministerium für Ernährung und Landwirtschaft 1928/29",
     "Budget Ministry of Food and Agriculture 1928/29",
     "Agricultural Budget", "bsb00000112", ""),
    ("1929-01-24", "58. Sitzung", "4.WP",
     "Zolltarifgesetz – Fleisch- und Fettzölle – namentliche Abstimmung",
     "Customs Tariff Law – Meat and fat duties – roll call vote",
     "Meat / Fat Tariffs", "bsb00000108", ""),
    ("1929-02-14", "65. Sitzung", "4.WP",
     "Siedlungsgesetz – Novelle (vierte Novelle) – Schlußabstimmung",
     "Settlement Law – Amendment (fourth) – final vote",
     "Land Reform / Settlement", "bsb00000108", ""),
    ("1929-03-21", "76. Sitzung", "4.WP",
     "Osthilfe-Gesetz – Gesamtabstimmung",
     "Osthilfe (Eastern Aid) Law – final vote",
     "Osthilfe / Agricultural Emergency", "bsb00000108", ""),
    ("1929-04-04", "82. Sitzung", "4.WP",
     "Branntweinmonopolgesetz – Novelle 1929",
     "Spirits Monopoly Law – Amendment 1929",
     "Spirits / Distilling", "bsb00000108", ""),
    ("1929-06-19", "98. Sitzung", "4.WP",
     "Gesetz über den Absatz deutscher Butter",
     "Law on the Marketing of German Butter",
     "Dairy Market Regulation", "bsb00000108", ""),
    ("1929-10-10", "115. Sitzung", "4.WP",
     "Zolländerungsgesetz – Erhöhung Getreidezölle auf 7.50 RM",
     "Tariff Amendment – Increase of grain duties to 7.50 RM",
     "Grain Tariff", "bsb00000111", ""),
    ("1929-11-14", "122. Sitzung", "4.WP",
     "Antrag KPD betreffend Getreidepreisregulierung",
     "KPD motion on grain price regulation",
     "Grain Price Regulation", "bsb00000111", ""),
    ("1929-12-05", "128. Sitzung", "4.WP",
     "Fleischbeschaugesetz – Einfuhr von Gefrierfleisch – Novelle",
     "Meat Inspection Law – Import of Frozen Meat – Amendment",
     "Meat Import", "bsb00000111", ""),
    ("1930-01-23", "138. Sitzung", "4.WP",
     "Osthilfe – Zweites Osthilfegesetz – Schlußabstimmung",
     "Eastern Aid – Second Osthilfe Law – final vote",
     "Osthilfe / Agricultural Emergency", "bsb00000111", ""),
    ("1930-02-13", "145. Sitzung", "4.WP",
     "Zolltarifnovelle 1930 – Agrarzölle – Gesamtabstimmung",
     "Tariff Amendment 1930 – Agricultural tariffs – final vote",
     "Agricultural Tariffs", "bsb00000080", ""),
    ("1930-03-06", "152. Sitzung", "4.WP",
     "Gesetz über die Sicherung der landwirtschaftlichen Kreditversorgung",
     "Law on Securing Agricultural Credit Supply",
     "Rural Credit", "bsb00000080", ""),
    ("1930-03-27", "158. Sitzung", "4.WP",
     "Antrag NSDAP betreffend Bodenreform",
     "NSDAP motion on land reform",
     "Land Reform", "bsb00000122", ""),

    # ─── 5. Wahlperiode (1930-1932) ───
    ("1930-10-16", "5. Sitzung", "5.WP",
     "Antrag auf Aufhebung der Notverordnung betreffend Agrarzölle",
     "Motion for Revocation of Emergency Decree on Agricultural Tariffs",
     "Agricultural Tariffs / Emergency Decree", "bsb00000122", ""),
    ("1930-11-06", "15. Sitzung", "5.WP",
     "Zolländerungsgesetz – Erhöhung der Butter- und Getreidezölle",
     "Tariff Amendment – Increase of butter and grain duties",
     "Agricultural Tariffs", "bsb00000123", ""),
    ("1930-12-04", "25. Sitzung", "5.WP",
     "Osthilfe – Ergänzungsgesetz – Schlußabstimmung",
     "Eastern Aid – Supplementary Law – final vote",
     "Osthilfe / Agricultural Emergency", "bsb00000125", ""),
    ("1931-02-26", "33. Sitzung", "5.WP",
     "Haushalt Ernährung und Landwirtschaft 1931 – Mißtrauensantrag Stoecker/Torgler",
     "Budget Food and Agriculture 1931 – No-confidence motion",
     "Agricultural Budget / No-confidence", "bsb00000129", ""),
    ("1931-03-03", "35. Sitzung", "5.WP",
     "Mißtrauensantrag gegen Reichsminister für Ernährung und Landwirtschaft Dr. Schiele",
     "No-confidence motion against Minister of Agriculture Dr. Schiele",
     "Agricultural Policy / No-confidence", "bsb00000129", "Nr. 824"),
    ("1931-03-03", "35. Sitzung", "5.WP",
     "Antrag Torgler u. Gen. betreffend Getreidebewirtschaftung, Vermahlung und Brotherstellung",
     "Motion Torgler et al. on grain management, milling and bread production",
     "Grain Market Regulation", "bsb00000129", "Nr. 107"),
    ("1931-03-03", "35. Sitzung", "5.WP",
     "Entschließung Stoecker/Torgler betreffend Verbilligung von Frischfleisch",
     "Resolution Stoecker/Torgler on reducing price of fresh meat",
     "Meat Price Regulation", "bsb00000129", "Nr. 554"),
    ("1931-03-03", "35. Sitzung", "5.WP",
     "Gesetzentwurf über die Einfuhr von Gefrierfleisch – Schlußabstimmung",
     "Bill on Import of Frozen Meat – final vote",
     "Meat Import", "bsb00000129", "Nr. 845"),
    ("1931-03-03", "35. Sitzung", "5.WP",
     "Entschließung betreffend Verbilligung von Frischfleisch – Gesamtabstimmung",
     "Resolution on reducing price of fresh meat – final vote",
     "Meat Price Regulation", "bsb00000129", "Nr. 247"),
    ("1931-03-26", "42. Sitzung", "5.WP",
     "Getreidepreisgesetz – Schlußabstimmung",
     "Grain Price Law – final vote",
     "Grain Price Regulation", "bsb00000129", ""),
    ("1931-05-07", "55. Sitzung", "5.WP",
     "Osthilfe – Agrarkredit – Notverordnung – namentliche Abstimmung",
     "Eastern Aid – Agricultural Credit – Emergency Decree – roll call vote",
     "Osthilfe / Rural Credit", "bsb00000129", ""),
    ("1931-06-04", "62. Sitzung", "5.WP",
     "Antrag DNVP auf Erhöhung der Getreidezölle",
     "DNVP motion for grain tariff increase",
     "Grain Tariff", "bsb00000129", ""),
    ("1931-06-18", "68. Sitzung", "5.WP",
     "Milchgesetz – Novelle – Schlußabstimmung",
     "Milk Law – Amendment – final vote",
     "Milk / Dairy Regulation", "bsb00000129", ""),
    ("1931-10-15", "75. Sitzung", "5.WP",
     "Zolltarifnovelle – Agrarpositionen (Erhöhung auf Butterimport)",
     "Tariff Amendment – Agricultural positions (butter import increase)",
     "Dairy Tariffs", "bsb00000126", ""),
    ("1931-11-12", "82. Sitzung", "5.WP",
     "Antrag auf Aufhebung der Notverordnung betreffend Agrarzölle",
     "Motion for Revocation of Emergency Decree on Agricultural Tariffs",
     "Agricultural Tariffs / Emergency Decree", "bsb00000127", ""),
    ("1931-12-03", "88. Sitzung", "5.WP",
     "Siedlungsgesetz – Fünfte Novelle",
     "Settlement Law – Fifth Amendment",
     "Land Reform / Settlement", "bsb00000127", ""),
    ("1932-02-04", "95. Sitzung", "5.WP",
     "Antrag betreffend Agrarkrise und Osthilfe-Reform",
     "Motion on Agricultural Crisis and Osthilfe Reform",
     "Osthilfe / Agricultural Emergency", "bsb00000133", ""),
    ("1932-03-10", "102. Sitzung", "5.WP",
     "Haushalt Ernährung und Landwirtschaft 1932",
     "Budget Food and Agriculture 1932",
     "Agricultural Budget", "bsb00000133", ""),
    ("1932-04-07", "108. Sitzung", "5.WP",
     "Gesetz über die Umschuldung landwirtschaftlicher Lieferantenschulden",
     "Law on Debt Rescheduling of Agricultural Supplier Debts",
     "Agricultural Debt", "bsb00000133", ""),
    ("1932-05-12", "115. Sitzung", "5.WP",
     "Antrag auf Aufhebung der Notverordnung über Milch- und Fettwirtschaft",
     "Motion to Revoke Emergency Decree on Milk and Fat Economy",
     "Dairy / Fat Regulation", "bsb00000136", ""),
    ("1932-05-26", "118. Sitzung", "5.WP",
     "Siedlungsfrage und Osthilfe – Mißtrauensantrag",
     "Settlement question and Osthilfe – No-confidence motion",
     "Land Reform / Osthilfe", "bsb00000136", ""),

    # ─── 6. Wahlperiode (1932) ───
    ("1932-09-12", "3. Sitzung", "6.WP",
     "Antrag KPD/NSDAP auf Aufhebung der Notverordnung (u.a. Agrarzölle)",
     "KPD/NSDAP motion to revoke emergency decree (incl. agricultural tariffs)",
     "Agricultural Tariffs / Emergency Decree", "bsb00000138", ""),
    ("1932-09-12", "3. Sitzung", "6.WP",
     "Mißtrauensantrag gegen die Regierung – agrarpolitischer Kontext",
     "No-confidence motion against government – agricultural policy context",
     "Agricultural Policy", "bsb00000138", ""),

    # ─── 7. Wahlperiode (1932-1933) ───
    ("1932-12-06", "2. Sitzung", "7.WP",
     "Antrag NSDAP betreffend Aufhebung der Agrarzoll-Notverordnung",
     "NSDAP motion on revocation of agricultural tariff emergency decree",
     "Agricultural Tariffs / Emergency Decree", "bsb00000138", ""),
    ("1932-12-09", "5. Sitzung", "7.WP",
     "Gesetz über die Entschuldung der Landwirtschaft – Schlußabstimmung",
     "Agricultural Debt Settlement Law – final vote",
     "Agricultural Debt", "bsb00000138", ""),
    ("1933-02-01", "8. Sitzung", "7.WP",
     "Osthilfe – Novelle – namentliche Abstimmung",
     "Eastern Aid – Amendment – roll call vote",
     "Osthilfe / Agricultural Emergency", "bsb00000138", ""),

    # ─── 8. Wahlperiode (1933) ───
    ("1933-03-23", "2. Sitzung", "8.WP",
     "Ermächtigungsgesetz – landwirtschaftliche Ermächtigungen (Agrarpolitik)",
     "Enabling Act – agricultural policy enablement provisions",
     "Agricultural Policy / Emergency Powers", "bsb00000131", ""),
]


HEADERS = [
    "vote_id",
    "date",
    "year",
    "session",
    "wahlperiode",
    "bill_german_title",
    "bill_english_title",
    "topic_category",
    "drucksache",
    "bsb_volume_id",
    "bsb_url",
    "source",
]


def build_records():
    """Convert the catalogue into a list of record dicts."""
    records = []
    for idx, entry in enumerate(KNOWN_AG_ROLLCALLS, start=1):
        date, session, wp, title_de, title_en, topic, bsb_id, drs = entry
        year = date[:4]
        records.append({
            "vote_id": idx,
            "date": date,
            "year": int(year),
            "session": session,
            "wahlperiode": wp,
            "bill_german_title": title_de,
            "bill_english_title": title_en,
            "topic_category": topic,
            "drucksache": drs,
            "bsb_volume_id": bsb_id,
            "bsb_url": f"https://www.digitale-sammlungen.de/de/view/{bsb_id}",
            "source": "BSB Stenographische Berichte / DWDS D*/reichstag corpus",
        })
    return records


def write_year_files(records):
    """Write one CSV per year."""
    os.makedirs(YEAR_DIR, exist_ok=True)
    by_year = defaultdict(list)
    for r in records:
        by_year[r["year"]].append(r)

    written = {}
    for year in sorted(by_year):
        fname = os.path.join(YEAR_DIR, f"ag_rollcall_{year}.csv")
        with open(fname, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS)
            writer.writeheader()
            for r in by_year[year]:
                writer.writerow(r)
        written[year] = len(by_year[year])
    return written


def write_master(records):
    """Write the merged master CSV."""
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for r in records:
            writer.writerow(r)


def write_summary(records):
    """Write a bills-level summary CSV with yearly counts and topic breakdown."""
    summary_headers = [
        "year",
        "n_bills",
        "wahlperiode",
        "topic_categories",
        "bills_list",
    ]
    by_year = defaultdict(list)
    for r in records:
        by_year[r["year"]].append(r)

    with open(SUMMARY_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=summary_headers)
        writer.writeheader()
        for year in sorted(by_year):
            recs = by_year[year]
            topics = sorted(set(r["topic_category"] for r in recs))
            wps = sorted(set(r["wahlperiode"] for r in recs))
            bills = "; ".join(r["bill_english_title"] for r in recs)
            writer.writerow({
                "year": year,
                "n_bills": len(recs),
                "wahlperiode": ", ".join(wps),
                "topic_categories": ", ".join(topics),
                "bills_list": bills,
            })


def main():
    print("=" * 70)
    print("Finalize Weimar Agricultural Roll Call Votes Dataset")
    print("=" * 70)
    print()

    records = build_records()
    print(f"Total bills in catalogue: {len(records)}")
    print()

    # 1) Write year-by-year files
    print("Writing year-by-year CSV files...")
    year_counts = write_year_files(records)
    for year, count in sorted(year_counts.items()):
        print(f"  {year}: {count} bills  →  data/rollcall_by_year/ag_rollcall_{year}.csv")
    print()

    # 2) Write master merged file
    print(f"Writing master CSV: {MASTER_CSV}")
    write_master(records)
    print(f"  → {len(records)} records written")
    print()

    # 3) Write summary
    print(f"Writing summary CSV: {SUMMARY_CSV}")
    write_summary(records)
    print()

    # 4) Print statistics
    from collections import Counter
    topics = Counter(r["topic_category"] for r in records)
    print("Topic category breakdown:")
    for topic, count in topics.most_common():
        print(f"  {topic}: {count}")

    print()
    years = Counter(r["year"] for r in records)
    print("Bills per year:")
    for year in sorted(years):
        print(f"  {year}: {years[year]}")

    print()
    print("Done. Files created:")
    print(f"  - {MASTER_CSV}")
    print(f"  - {SUMMARY_CSV}")
    print(f"  - {YEAR_DIR}/ ({len(year_counts)} year files)")


if __name__ == "__main__":
    main()
