"""
Extract individual-level roll call votes on Weimar agricultural bills
from the BSB Digitale Sammlungen OCR API.

For each of the 119 catalogued agricultural roll call votes, this script:
  1. Searches the BSB IIIF Search API to find "Zusammenstellung der
     namentlichen Abstimmung" pages in the corresponding volume.
  2. Downloads OCR text (hOCR word-level) for those pages.
  3. Parses individual MP names, party affiliations, and vote choices
     (Ja / Nein / Enthalten / krank / beurlaubt / fehlt).
  4. Writes year-by-year CSVs into data/individual_by_year/.
  5. Writes a merged master CSV: weimar_ag_individual_votes_master.csv

Primary data source:
  https://www.digitale-sammlungen.de/en/german-reichstag-session-reports-including-database-of-members/about
  Licence: CC BY-SA 4.0

Usage:
    python create_individual_rollcall_data.py
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
INDIVIDUAL_YEAR_DIR = os.path.join(SCRIPT_DIR, "data", "individual_by_year")
MASTER_CSV = os.path.join(SCRIPT_DIR, "weimar_ag_individual_votes_master.csv")

HTTP_TIMEOUT = 30
API_DELAY = 0.3  # seconds between API requests

# ─── Known agricultural roll call votes catalogue ──────────────────────────
# Each entry: (date, session, wahlperiode, title_de, title_en,
#              topic_category, bsb_id, canvas_range_or_None, drucksache)

KNOWN_AG_ROLLCALLS = [
    # ─── WNV (1919-1920) ───
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

    # ─── 1.WP (1920-1924) ───
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

    # ─── 2.WP (Mai-Okt 1924) ───
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

    # ─── 3.WP (Dez 1924 - 1928) ───
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

    # ─── 4.WP (1928-1930) ───
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

    # ─── 5.WP (1930-1932) ───
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

    # ─── 6.WP (1932) ───
    ("1932-09-12", "3. Sitzung", "6.WP",
     "Antrag KPD/NSDAP auf Aufhebung der Notverordnung (u.a. Agrarzölle)",
     "KPD/NSDAP motion to revoke emergency decree (incl. agricultural tariffs)",
     "Agricultural Tariffs / Emergency Decree", "bsb00000138", None, ""),
    ("1932-09-12", "3. Sitzung", "6.WP",
     "Mißtrauensantrag gegen die Regierung – agrarpolitischer Kontext",
     "No-confidence motion against government – agricultural policy context",
     "Agricultural Policy", "bsb00000138", None, ""),

    # ─── 7.WP (1932-1933) ───
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

    # ─── 8.WP (1933) ───
    ("1933-03-23", "2. Sitzung", "8.WP",
     "Ermächtigungsgesetz – landwirtschaftliche Ermächtigungen (Agrarpolitik)",
     "Enabling Act – agricultural policy enablement provisions",
     "Agricultural Policy / Emergency Powers", "bsb00000131", None, ""),
]

# ─── Weimar parties ───────────────────────────────────────────────────────

WEIMAR_PARTIES = [
    ("SPD", r"Sozialdemokratische\s+Partei|Sozialdemokrat"),
    ("USPD", r"Unabhängige\s+Sozialdemokrat"),
    ("KPD", r"Kommunistische\s+Partei|Kommunist"),
    ("Zentrum", r"Zentrum(?:spartei)?|Zentrums?(?:fraktion)?"),
    ("BVP", r"Bayerische\s+Volkspartei"),
    ("DDP", r"Deutsche\s+Demokratische\s+Partei"),
    ("DStP", r"Deutsche\s+Staatspartei"),
    ("DVP", r"Deutsche\s+Volkspartei"),
    ("DNVP", r"Deutschnationale\s+Volkspartei|Deutschnationale"),
    ("NSDAP", r"Nationalsozialisti"),
    ("WP", r"Wirtschaftspartei|Reichspartei\s+des\s+deutschen\s+Mittelstandes"),
    ("Landvolk", r"Deutsches\s+Landvolk|Christlich-Nationale\s+Bauern"),
    ("Landbund", r"Landbund|Deutsche\s+Bauernpartei"),
    ("CSVD", r"Christlich-Sozialer\s+Volksdienst"),
    ("Volksrechtspartei", r"Volksrechtspartei"),
    ("Fraktionslos", r"Fraktionslos|fraktionslos|Parteilos"),
    ("Gäste", r"G[äa]ste"),
]

# ─── HTTP helpers ─────────────────────────────────────────────────────────

def http_get_json(url):
    """GET and parse JSON."""
    try:
        req = urllib.request.Request(
            url, headers={"Accept": "application/json",
                          "User-Agent": "landpolicies-research/1.0"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def http_get_text(url):
    """GET and return text."""
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "landpolicies-research/1.0"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None


# ─── BSB API ──────────────────────────────────────────────────────────────

def bsb_search(bsb_id, query):
    """Search within a BSB volume. Returns list of (canvas, context_text)."""
    url = (f"https://api.digitale-sammlungen.de/iiif/services/search/v1/"
           f"{bsb_id}?q={urllib.parse.quote(query)}")
    results = []
    for _ in range(3):  # max 3 pagination pages
        data = http_get_json(url)
        if not data:
            break
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
    html = http_get_text(url)
    if not html:
        return ""
    words = re.findall(r'class="ocrx_word"[^>]*>([^<]+)', html)
    return " ".join(words)


def find_zusammenstellung_canvases(bsb_id):
    """Find canvas numbers of Zusammenstellung pages in a volume.

    The actual Zusammenstellung pages contain the heading
    'Zusammenstellung der namentlichen Abstimmung(en)' followed by
    a table of party faction headers and MP names with vote columns.

    We validate by checking that the OCR text of candidate pages
    actually contains party faction headers (SPD, DNVP, etc.) which
    are a strong signal of being a real vote tally page.
    """
    results = bsb_search(bsb_id, "Zusammenstellung namentlichen Abstimmung")
    candidates = set()
    for canvas, context in results:
        if canvas and canvas > 10:  # skip table of contents
            if "Zusammenstellung" in context:
                candidates.add(canvas)

    if not candidates:
        return []

    # Validate candidates: check if at least one candidate page contains
    # party faction headers (hallmark of a Zusammenstellung page)
    party_patterns = [
        "Sozialdemokrat", "Kommunist", "Zentrum", "Deutschnational",
        "Deutsche Volkspartei", "Bayerische Volkspartei",
        "Wirtschaftspartei", "Nationalsozial", "Landvolk", "Landbund",
    ]

    validated = set()
    for canvas in sorted(candidates):
        text = bsb_get_ocr(bsb_id, canvas)
        if not text:
            continue
        time.sleep(API_DELAY)

        # Check for party headers — strong signal of vote tally page
        party_count = sum(1 for p in party_patterns if p.lower() in text.lower())
        if party_count >= 2:
            # This is a real Zusammenstellung page
            validated.add(canvas)
            # Also add adjacent pages (vote lists span multiple pages)
            for offset in range(1, 5):
                validated.add(canvas + offset)
        elif "Name" in text and ("Abstimmung" in text or "Ja" in text):
            # Might be a continuation page
            validated.add(canvas)

    return sorted(validated)


# ─── Vote parsing ─────────────────────────────────────────────────────────

def normalise_vote(raw):
    """Normalise OCR vote text to canonical form."""
    raw = raw.strip().strip(".,!;:^'\"()[]|/\\")
    low = raw.lower()
    # Direct matches
    if raw in ("Ja", "ja", "JA", "Za", "Fa", "Ia", "za", "fa", "ia"):
        return "Ja"
    if raw in ("1", "I", "l", "|") and len(raw) == 1:
        return "Ja"
    if "nein" in low or "illein" in low or raw in ("Mm", "Min", "New", "Neu"):
        return "Nein"
    if "krank" in low:
        return "krank"
    if "beurl" in low or "benrl" in low:
        return "beurlaubt"
    if "enthalt" in low:
        return "Enthalten"
    if "fehlt" in low:
        return "fehlt"
    if raw in ("—", "-", "–", "....", "...."):
        return "absent"
    if "ja" in low and len(raw) <= 4:
        return "Ja"
    return raw


def detect_party(text):
    """Detect party from text snippet."""
    for abbrev, pattern in WEIMAR_PARTIES:
        if re.search(pattern, text, re.IGNORECASE):
            return abbrev
    return ""


def parse_zusammenstellung_header(text):
    """Parse the header to find vote descriptions and count.
    Returns (session_info, list of vote descriptions)."""
    # Find number of votes from column headers like "1. 2. 3. 4. 5. Abstimmung"
    m = re.search(r"(\d+)\.\s*(?:und\s+\d+\.\s*)?Abstimmung", text)
    if m:
        # but really we should count from "1. Name 2. 3. 4. Abstimmung"
        pass

    # Find descriptions "1. über ..." "2. über ..."
    vote_descs = []
    for m in re.finditer(r"(\d+)\.\s+(über\s+.{10,200}?)(?=\d+\.\s+(?:über|Schlu|Gesamt|bei)|$)",
                         text, re.IGNORECASE | re.DOTALL):
        vote_descs.append((int(m.group(1)), re.sub(r"\s+", " ", m.group(2).strip())))

    return vote_descs


    # Words that are never MP names (OCR noise from headers, page text)
NOISE_WORDS = {
    "Reichstag", "Sitzung", "Dienstag", "Montag", "Mittwoch", "Donnerstag",
    "Freitag", "Sonnabend", "Zusammenstellung", "Abstimmung", "Name",
    "Abgegebene", "Stimmzettel", "Ungültig", "Bleiben", "Davon", "Summe",
    "Druck", "Verlag", "Reichsdruckerei", "Namentliche", "Abstimmungen",
    "Nationalversammlung", "Schlußabstimmung", "Gesamtabstimmung",
    "Drucksachen", "Beratung", "Gesetzentwurf", "Antrag", "Entschließung",
    "Genossen", "Ausschuß", "Volkswirtschaft", "Mißtrauensantrag",
    "Mündlichen", "Berichts", "Beschlüsse", "Anträge", "Gesetz",
    "Entwurfs", "Entwurf", "Novelle", "Erhöhung", "Aufhebung",
    "Einfuhr", "Ausfuhr", "Haushalt", "Einzelplan", "Bevölkerung",
    "Verbilligung", "Bewirtschaftung", "Vermahlung", "Brotherstellung",
    "Brotberstellnng", "Brvthersteltnng", "Gefrierfleisch", "Frischfleisch",
    "Getreidebewirtschaftung", "Getreide", "Siedlung", "Siedlungsgesetz",
    "Osthilfe", "Landwirtschaft", "Ernährung", "Zolltarif", "Zolltarifgesetz",
    "Zolländerungsgesetz", "Branntweinmonopol", "Viehseuchen", "Milch",
    "Zucker", "Hypotheken", "Rentenbank", "Kreditversorgung", "Bodenreform",
    "Hört", "Sehr", "Bravo", "Rufe", "Beifall", "Zuruf", "Abgeordneten",
    "Vizepräsident", "Präsident", "Minister", "Reichsminister",
    "Reichsininister", "Reichsverkehrsminister", "Reichskanzler",
    "Meine", "Damen", "Herren", "Ich", "Wir", "Die", "Der", "Das", "Den",
    "Dem", "Des", "Ein", "Eine", "Einer", "Eines", "Einem", "Einen",
    "Und", "Oder", "Aber", "Auch", "Nur", "Noch", "Schon", "Dann",
    "Diese", "Dieser", "Dieses", "Diesem", "Diesen", "Jene", "Jener",
    "Nach", "Vor", "Bei", "Mit", "Von", "Aus", "Für", "Auf", "In",
    "Über", "Unter", "Durch", "Gegen", "Ohne", "Zwischen", "Seit",
    "Berlin", "Norddeutschen", "Buchdruckerei", "Verlagsanstalt",
    "Wilhelmstraße", "März", "April", "Mai", "Juni", "Juli", "August",
    "September", "Oktober", "November", "Dezember", "Januar", "Februar",
    "Demobilisation", "Sozialisierung", "Sozialisierungsgesetz",
}


def is_plausible_mp_name(name):
    """Check if a string looks like a plausible MP name (not OCR noise)."""
    if not name or len(name) < 3:
        return False
    # Reject single common words
    if name in NOISE_WORDS:
        return False
    # Reject if all parts are noise words
    parts = name.split()
    non_noise = [p for p in parts if p not in NOISE_WORDS and len(p) > 1]
    if not non_noise:
        return False
    # Reject if it contains too many lowercase-starting words (sentence fragment)
    if len(parts) > 3:
        lc_count = sum(1 for p in parts if p[0].islower() and p not in ("von", "v.", "zu", "der", "den"))
        if lc_count > len(parts) // 2:
            return False
    # Reject very long "names" (likely sentence fragments)
    if len(name) > 60:
        return False
    # Reject names that are really numbers
    if re.match(r"^\d+$", name):
        return False
    return True


def parse_individual_votes_from_text(text, num_votes=1):
    """Parse individual MP votes from OCR text of Zusammenstellung pages.

    The format is a table organised by party faction:
        Party Header
        Name [dots/spaces] Vote1 Vote2 ...

    Vote values can be: Ja, Nein, 1, krank, beurl., fehlt, —

    Returns list of {mp_name, party, vote, vote_col}.
    """
    records = []
    current_party = ""

    # Strip header text: everything before the first party header is
    # description text (vote titles, session info). Only parse after
    # the first party faction heading or "Name ... Abstimmung" table header.
    party_header_re = (
        r"(Sozialdemokratische|Kommunistische|Zentrum(?:spartei|sfraktion)?\b|"
        r"Bayerische\s+Volkspartei|Deutsche\s+Demokratische|Deutsche\s+Staatspartei|"
        r"Deutsche\s+Volkspartei|Deutschnationale|Nationalsozialisti|"
        r"Wirtschaftspartei|Reichspartei\s+des|Deutsches\s+Landvolk|"
        r"Landbund|Christlich-Nationale|Christlich-Sozialer|"
        r"Fraktionslos|Parteilos)"
    )
    # Find the first party header
    m = re.search(party_header_re, text)
    if m:
        text = text[m.start():]
    else:
        # No party header found — try "Name" column header
        m2 = re.search(r"\bName\b.*\bAbstimmung\b", text[:500])
        if m2:
            text = text[m2.end():]
        # If neither found, the text might not contain a vote list at all

    # Split on party headers
    party_pattern = r"(?=" + party_header_re + r")"
    sections = re.split(party_pattern, text)

    for section in sections:
        if not section.strip():
            continue

        # Check for party header
        party = detect_party(section[:120])
        if party:
            current_party = party

        # Extract name-vote pairs
        words = section.split()
        i = 0
        while i < len(words):
            w = words[i]

            # Skip page headers, numbers, noise
            if re.match(r"^\d{4}$", w):  # year/page number
                i += 1
                continue
            if w in NOISE_WORDS:
                i += 1
                continue

            # Try to build a name
            name_parts = []
            j = i
            while j < len(words):
                word = words[j]
                norm = normalise_vote(word)

                # Is this word a vote value?
                if norm in ("Ja", "Nein", "krank", "beurlaubt", "Enthalten", "fehlt", "absent"):
                    break

                # Is this word name-like?
                if (re.match(r"^(?:Dr\.|Frau|Graf|von|Fhr\.|Frhr\.|Fürst|D\.|Dr\b)", word) or
                    re.match(r"^[A-ZÄÖÜ]", word) or
                    word in (".", "-", "(", ")", "rc.", "u.", "Gen.", "v.")):
                    name_parts.append(word)
                    j += 1
                elif re.match(r"^\d{1,2}$", word):
                    # Could be a column number marker in tabular format
                    j += 1
                    continue
                else:
                    break

            if not name_parts:
                i += 1
                continue

            # Collect vote values
            votes = []
            while j < len(words) and len(votes) < max(num_votes + 2, 8):
                word = words[j]
                norm = normalise_vote(word)
                if norm in ("Ja", "Nein", "krank", "beurlaubt", "Enthalten", "fehlt", "absent"):
                    votes.append(norm)
                    j += 1
                elif re.match(r"^[A-ZÄÖÜ]", word) and norm not in ("Ja", "Nein"):
                    # Looks like start of next name
                    break
                else:
                    j += 1
                    # If we've passed too many non-vote words, stop
                    if j - (i + len(name_parts)) > len(votes) + 5:
                        break

            # Clean up name
            name = " ".join(name_parts).strip(" .-,;:()")
            name = re.sub(r"\s+", " ", name)
            name = re.sub(r"^\d+\s*", "", name)

            # Filter out noise
            if name and votes and is_plausible_mp_name(name):
                # Don't include party names as MP names
                if not detect_party(name):
                    for v_idx, vote_val in enumerate(votes):
                        records.append({
                            "mp_name": name,
                            "party": current_party,
                            "vote": vote_val,
                            "vote_col": v_idx + 1,
                        })

            i = max(j, i + 1)

    return records


# ─── Main pipeline ────────────────────────────────────────────────────────

def extract_individual_votes_for_bill(bsb_id, canvas_range, session_str):
    """Extract individual votes for a bill from BSB OCR.

    Returns list of {mp_name, party, vote, vote_col} or empty list.
    """
    if canvas_range:
        start_c, end_c = canvas_range
        canvases = list(range(start_c, end_c + 1))
    else:
        # Search for Zusammenstellung pages
        canvases = find_zusammenstellung_canvases(bsb_id)
        if not canvases:
            return []

    # Download OCR for the Zusammenstellung pages
    all_text = ""
    for c in canvases:
        text = bsb_get_ocr(bsb_id, c)
        if not text:
            continue
        all_text += " " + text
        # Stop if we hit the end marker
        if "Druck und Verlag" in text and "Reichsdruckerei" in text:
            break
        time.sleep(API_DELAY)

    if not all_text or len(all_text) < 100:
        return []

    # Determine how many simultaneous votes (from column headers)
    num_votes = 1
    m = re.search(r"(\d+)\.\s+(?:und\s+\d+\.\s+)?Abstimmung", all_text)
    if m:
        # Check for "1. Name 2. 3. 4. 5. Abstimmung" pattern
        cols = re.findall(r"(\d+)\.", all_text[:500])
        if cols:
            num_votes = max(1, max(int(c) for c in cols if int(c) < 20) - 1)
            # The "Name" is column 1, votes are columns 2+
            # Actually look for explicit "1. 2. 3. ... Abstimmung"
            m2 = re.search(r"1\.\s*Name\s*((?:\d+\.\s*)+)Abstimmung", all_text[:500])
            if m2:
                vote_cols = re.findall(r"(\d+)\.", m2.group(1))
                if vote_cols:
                    num_votes = len(vote_cols)

    return parse_individual_votes_from_text(all_text, num_votes)


def main():
    print("=" * 70)
    print("Weimar Agricultural Roll Call Votes – Individual-Level Extraction")
    print("=" * 70)
    print()

    os.makedirs(INDIVIDUAL_YEAR_DIR, exist_ok=True)

    all_individual_records = []
    bills_with_data = 0
    bills_without_data = 0

    # Group bills by BSB volume to avoid redundant API calls
    volume_cache = {}  # bsb_id -> list of Zusammenstellung canvases

    total = len(KNOWN_AG_ROLLCALLS)
    for idx, entry in enumerate(KNOWN_AG_ROLLCALLS):
        date, session, wp, title_de, title_en, topic, bsb_id, canvas_range, drs = entry
        year = date[:4]
        vote_id = idx + 1

        print(f"  [{idx+1}/{total}] {date} | {title_de[:55]}...")

        # Try to extract individual votes
        individual_votes = []
        try:
            individual_votes = extract_individual_votes_for_bill(
                bsb_id, canvas_range, session)
        except Exception as e:
            print(f"    Error: {e}")

        if individual_votes:
            bills_with_data += 1
            print(f"    → {len(individual_votes)} individual vote records extracted")

            for rec in individual_votes:
                all_individual_records.append({
                    "vote_id": vote_id,
                    "date": date,
                    "year": int(year),
                    "session": session,
                    "wahlperiode": wp,
                    "bill_german_title": title_de,
                    "bill_english_title": title_en,
                    "topic_category": topic,
                    "drucksache": drs,
                    "mp_name": rec["mp_name"],
                    "mp_party": rec["party"],
                    "vote": rec["vote"],
                    "vote_column": rec["vote_col"],
                    "bsb_volume_id": bsb_id,
                    "bsb_url": f"https://www.digitale-sammlungen.de/de/view/{bsb_id}",
                    "source": "BSB OCR extraction",
                })
        else:
            bills_without_data += 1
            print(f"    → No individual data extracted (metadata only)")

            # Still record the bill metadata
            all_individual_records.append({
                "vote_id": vote_id,
                "date": date,
                "year": int(year),
                "session": session,
                "wahlperiode": wp,
                "bill_german_title": title_de,
                "bill_english_title": title_en,
                "topic_category": topic,
                "drucksache": drs,
                "mp_name": "",
                "mp_party": "",
                "vote": "",
                "vote_column": "",
                "bsb_volume_id": bsb_id,
                "bsb_url": f"https://www.digitale-sammlungen.de/de/view/{bsb_id}",
                "source": "catalogue_metadata_only",
            })

        time.sleep(API_DELAY)

    # ── Write year-by-year files ──────────────────────────────────────
    print()
    print("Writing year-by-year individual vote CSVs...")

    headers = [
        "vote_id", "date", "year", "session", "wahlperiode",
        "bill_german_title", "bill_english_title", "topic_category",
        "drucksache", "mp_name", "mp_party", "vote", "vote_column",
        "bsb_volume_id", "bsb_url", "source",
    ]

    by_year = defaultdict(list)
    for rec in all_individual_records:
        by_year[rec["year"]].append(rec)

    for year in sorted(by_year):
        fname = os.path.join(INDIVIDUAL_YEAR_DIR, f"ag_individual_{year}.csv")
        with open(fname, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for rec in by_year[year]:
                writer.writerow(rec)
        n_mp = sum(1 for r in by_year[year] if r["mp_name"])
        n_bills = len(set(r["vote_id"] for r in by_year[year]))
        print(f"  {year}: {n_bills} bills, {n_mp} individual vote records  →  {fname}")

    # ── Write master merged CSV ───────────────────────────────────────
    print()
    print(f"Writing master CSV: {MASTER_CSV}")
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for rec in all_individual_records:
            writer.writerow(rec)

    # ── Summary ───────────────────────────────────────────────────────
    total_individual = sum(1 for r in all_individual_records if r["mp_name"])
    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"  Total bills processed:                {total}")
    print(f"  Bills with individual vote data:       {bills_with_data}")
    print(f"  Bills with metadata only:              {bills_without_data}")
    print(f"  Total individual vote records:         {total_individual}")
    print(f"  Total records (incl. metadata-only):   {len(all_individual_records)}")
    print()
    print(f"Output files:")
    print(f"  Master: {MASTER_CSV}")
    print(f"  By year: {INDIVIDUAL_YEAR_DIR}/")
    print()
    print(f"Data source: BSB Digitale Sammlungen (CC BY-SA 4.0)")
    print(f"  https://www.digitale-sammlungen.de/en/german-reichstag-session-reports-including-database-of-members/about")


if __name__ == "__main__":
    main()
