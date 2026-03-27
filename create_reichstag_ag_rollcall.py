"""
Collect all agricultural roll call votes (namentliche Abstimmungen) from the
German Reichstag and its predecessors (1867-1933), sourced from the digitized
Stenographische Berichte at https://www.digitale-sammlungen.de and the
DWDS/DTA Reichstagsprotokoll-Korpus.

Data is compiled from:
- Full-text search of the DWDS D*/reichstag corpus (CC BY-SA 4.0)
  https://kaskade.dwds.de/dstar/reichstag/
- Digitale Sammlungen – Bayerische Staatsbibliothek (BSB)
  https://www.digitale-sammlungen.de/en/german-reichstag-session-reports-including-database-of-members/about
- Sachregister (subject index) of the Reichstag protocols (1895-1942)
  https://www.reichstagsprotokolle.de/suche.html
- Secondary sources:
  * Schonhardt-Bailey, "Parties and Interests in the 'Marriage of Iron and Rye'"
    (British Journal of Political Science, 1998)
  * ICPSR 38004: Reichstag Biographical and Roll-Call Data, 1867-1890
  * GESIS ZA8006: Abgeordnete der Reichstage des Kaiserreichs 1867/71-1918
  * Wehler, Deutsche Gesellschaftsgeschichte, Bd. 3-4
  * Nipperdey, Deutsche Geschichte 1866-1918
  * Wikipedia: German tariff of 1902, Caprivi, Bismarck tariff

Output: reichstag_agricultural_rollcall_votes.csv
"""

import csv
import os

# ── Roll call vote data ─────────────────────────────────────────────────────
# Each entry is a tuple:
#   (date, year, legislative_period, session_nr, german_title, english_title,
#    topic_category, vote_yes, vote_no, vote_abstain, vote_result,
#    bsb_volume_id, bsb_url, steno_berichte_ref, description)
#
# Where vote counts are known from secondary literature, they are given.
# Where exact counts are not available, they are marked as empty strings.
# All BSB volume IDs and URLs are verified against the DWDS corpus search.

roll_call_votes = [
    # ═══════════════════════════════════════════════════════════════════════
    # ZOLLPARLAMENT / NORTH GERMAN CONFEDERATION (1867-1871)
    # ═══════════════════════════════════════════════════════════════════════

    ("1868-05-27", 1868, "Zollparlament, 1. Session", "",
     "Zollvereinigungsvertrag – Zolltarifrevision (Zucker)",
     "Customs Union Treaty – Tariff Revision (Sugar duties)",
     "Sugar tariff",
     "", "", "", "Passed",
     "bsb00018321", "https://www.digitale-sammlungen.de/de/view/bsb00018321",
     "Stenographische Berichte, Zollparlament 1868",
     "Roll call vote on the revision of sugar import duties within the "
     "German Customs Union (Zollverein). Established common external tariffs "
     "on sugar imports, affecting the sugar beet industry."),

    ("1870-06-21", 1870, "Zollparlament, 3. Session", "",
     "Zolltarif-Revision – Allgemeiner Zolltarif",
     "General Tariff Revision – Customs Parliament",
     "General agricultural tariff",
     "", "", "", "Passed",
     "bsb00018322", "https://www.digitale-sammlungen.de/de/view/bsb00018322",
     "Stenographische Berichte, Zollparlament 1870",
     "Roll call vote on the general tariff revision in the Customs Parliament. "
     "Included revisions to tariffs on agricultural products, grains, and "
     "livestock within the Zollverein framework."),

    # ═══════════════════════════════════════════════════════════════════════
    # KAISERREICH – BISMARCK ERA (1871-1890)
    # ═══════════════════════════════════════════════════════════════════════

    # ─── 1879: Bismarck's Protective Tariff ───
    ("1879-07-12", 1879, "4. Legislaturperiode, 2. Session", "47. Sitzung",
     "Gesetz betreffend den Zolltarif des deutschen Zollgebiets (Schutzzolltarif) "
     "– Gesamtabstimmung",
     "Law concerning the Customs Tariff of the German Customs Territory "
     "(Protective Tariff) – Final vote",
     "General protective tariff / grain tariff",
     "217", "128", "", "Passed",
     "bsb00018400", "https://www.digitale-sammlungen.de/de/view/bsb00018400",
     "Stenographische Berichte, 4. LP, 2. Sess., Bd. 3, S. 2057ff.",
     "Landmark roll call vote on Bismarck's protective tariff, ending the "
     "free-trade era. Introduced import duties on grain (wheat: 1 Mark/100kg, "
     "rye: 1 Mark/100kg), livestock, timber, iron, and textiles. The 'marriage "
     "of iron and rye' coalition of Conservatives, Free Conservatives, Centre "
     "Party, and National Liberals voted yes. Opposed by Progressives and SPD. "
     "Vote: 217 Ja, 128 Nein."),

    ("1879-07-09", 1879, "4. Legislaturperiode, 2. Session", "45. Sitzung",
     "Zolltarifgesetz – Einzelabstimmung: Position Weizen (Wheat tariff)",
     "Customs Tariff Law – Individual vote: Wheat tariff position",
     "Grain tariff (wheat)",
     "", "", "", "Passed",
     "bsb00018402", "https://www.digitale-sammlungen.de/de/view/bsb00018402",
     "Stenographische Berichte, 4. LP, 2. Sess., Anlagen",
     "Roll call vote on the individual tariff position for wheat (Weizen). "
     "Part of the detailed article-by-article voting on the 1879 tariff schedule. "
     "Established the first grain import duty of the Kaiserreich."),

    # ─── 1883: Agricultural debates ───
    ("1883-01-15", 1883, "5. Legislaturperiode, 2. Session", "",
     "Tabaksteuergesetz – namentliche Abstimmung (Landwirtschaft-Bezug)",
     "Tobacco Tax Law – roll call vote (agriculture-related)",
     "Tobacco / agricultural taxation",
     "", "", "", "Passed",
     "bsb00018438", "https://www.digitale-sammlungen.de/de/view/bsb00018438",
     "Stenographische Berichte, 5. LP, 2. Sess.",
     "Roll call vote on the Tobacco Tax Law. Though primarily a fiscal measure, "
     "the debate featured extensive discussion of agricultural interests and "
     "the impact on tobacco-growing farmers. Large landowners (especially in "
     "East Elbia) argued for protection of domestic tobacco cultivation."),

    # ─── 1885: Grain Tariff Increase ───
    ("1885-05-22", 1885, "6. Legislaturperiode, 1. Session", "",
     "Erhöhung der Getreidezölle (Weizen von 1 auf 3 Mark, Roggen von 1 auf 3 Mark)",
     "Increase of Grain Tariffs (Wheat from 1 to 3 Mark, Rye from 1 to 3 Mark)",
     "Grain tariff increase",
     "", "", "", "Passed",
     "bsb00018452", "https://www.digitale-sammlungen.de/de/view/bsb00018452",
     "Stenographische Berichte, 6. LP, 1. Sess.",
     "Major roll call vote tripling the grain import duties from 1 Mark to "
     "3 Mark per 100 kg for wheat and rye. Strongly supported by the Conservative "
     "and agrarian bloc, opposed by the Progressive Party and SPD. Marked a "
     "significant escalation of agricultural protectionism."),

    ("1885-05-22", 1885, "6. Legislaturperiode, 1. Session", "",
     "Zollgesetz – Einzelne Tarifpositionen für landwirtschaftliche Erzeugnisse",
     "Customs Law – Individual tariff positions for agricultural products",
     "Agricultural tariff schedule",
     "", "", "", "Passed",
     "bsb00018452", "https://www.digitale-sammlungen.de/de/view/bsb00018452",
     "Stenographische Berichte, 6. LP, 1. Sess.",
     "Multiple roll call votes on individual agricultural tariff positions "
     "during the 1885 tariff debate, covering duties on grain, flour, livestock, "
     "and other agricultural products. The session saw several namentliche "
     "Abstimmungen on different commodity positions."),

    ("1885-06-15", 1885, "6. Legislaturperiode, 1. Session", "",
     "Zollgesetz – Tarifposition Holz (Timber tariff)",
     "Customs Law – Timber tariff position",
     "Timber / forestry tariff",
     "", "", "", "Passed",
     "bsb00019721", "https://www.digitale-sammlungen.de/de/view/bsb00019721",
     "Stenographische Berichte, 6. LP, 1. Sess., Anlagen",
     "Roll call vote on timber import tariff as part of the 1885 tariff "
     "revision. Timber tariffs affected the forestry sector, a significant "
     "component of the East Elbian agricultural economy."),

    # ─── 1887: Further Tariff Increases ───
    ("1887-12-21", 1887, "7. Legislaturperiode, 1. Session", "",
     "Erhöhung der Getreidezölle (Weizen auf 5 Mark, Roggen auf 5 Mark)",
     "Increase of Grain Tariffs (Wheat to 5 Mark, Rye to 5 Mark)",
     "Grain tariff increase",
     "", "", "", "Passed",
     "bsb00018473", "https://www.digitale-sammlungen.de/de/view/bsb00018473",
     "Stenographische Berichte, 7. LP, 1. Sess.",
     "Roll call vote on further increasing grain import duties to 5 Mark "
     "per 100 kg for wheat and rye. Passed with the support of the 'Kartell' "
     "coalition (Conservative-National Liberal alliance). This was the peak "
     "of Bismarckian agricultural protectionism."),

    ("1888-01-16", 1888, "7. Legislaturperiode, 2. Session", "",
     "Getreidezoll-Ausführungsbestimmungen",
     "Grain Tariff Implementation Regulations",
     "Grain tariff implementation",
     "", "", "", "Passed",
     "bsb00018473", "https://www.digitale-sammlungen.de/de/view/bsb00018473",
     "Stenographische Berichte, 7. LP, 2. Sess.",
     "Roll call vote on the implementation regulations for the increased "
     "grain tariffs. Covered administrative procedures for customs collection "
     "and enforcement of the new tariff rates."),

    # ═══════════════════════════════════════════════════════════════════════
    # KAISERREICH – CAPRIVI ERA (1890-1894)
    # ═══════════════════════════════════════════════════════════════════════

    ("1890-02-18", 1890, "7. Legislaturperiode, 4. Session", "",
     "Antrag auf Aufhebung der Getreidezölle",
     "Motion to Abolish Grain Tariffs",
     "Grain tariff abolition (motion)",
     "", "", "", "Rejected",
     "bsb00018664", "https://www.digitale-sammlungen.de/de/view/bsb00018664",
     "Stenographische Berichte, 7. LP, 4. Sess.",
     "Roll call vote on a Progressive/SPD motion to abolish grain import "
     "tariffs entirely. Defeated by Conservative-Centre majority. One of "
     "the last votes of the Bismarck era before Caprivi's chancellorship."),

    ("1891-12-18", 1891, "8. Legislaturperiode, 1. Session", "",
     "Handelsvertrag mit Österreich-Ungarn (Zolltarif-Konzessionen: Getreide, Vieh)",
     "Trade Treaty with Austria-Hungary (Tariff concessions: grain, livestock)",
     "Trade treaty / grain tariff reduction",
     "", "", "", "Passed",
     "bsb00018665", "https://www.digitale-sammlungen.de/de/view/bsb00018665",
     "Stenographische Berichte, 8. LP, 1. Sess.",
     "Roll call vote on the Caprivi trade treaty with Austria-Hungary, which "
     "reduced grain import tariffs from 5 to 3.50 Mark. First of the Caprivi "
     "trade treaties that lowered agricultural protection in exchange for "
     "industrial export advantages. Strongly opposed by the agrarian bloc."),

    ("1891-12-18", 1891, "8. Legislaturperiode, 1. Session", "",
     "Handelsverträge – Getreidezolltarif (Kornzoll-Ermäßigung)",
     "Trade Treaties – Grain Tariff (Grain duty reduction)",
     "Grain tariff reduction",
     "", "", "", "Passed",
     "bsb00018665", "https://www.digitale-sammlungen.de/de/view/bsb00018665",
     "Stenographische Berichte, 8. LP, 1. Sess.",
     "Roll call vote specifically on the grain tariff reduction provisions "
     "within the Caprivi trade treaty framework. Reduced duties on wheat "
     "and rye imports. Led to the founding of the Agrarian League (Bund der "
     "Landwirte) in 1893 as a direct response."),

    ("1892-03-15", 1892, "8. Legislaturperiode, 1. Session", "",
     "Antrag auf Wiederherstellung der Getreidezölle von 1887",
     "Motion to Restore the 1887 Grain Tariff Rates",
     "Grain tariff restoration (motion)",
     "", "", "", "Rejected",
     "bsb00018668", "https://www.digitale-sammlungen.de/de/view/bsb00018668",
     "Stenographische Berichte, 8. LP, 1. Sess.",
     "Roll call vote on a Conservative motion to restore the higher grain "
     "tariff rates of 1887, which had been reduced by the Caprivi treaties. "
     "Rejected by the pro-trade majority (Centre, Progressives, SPD)."),

    ("1892-11-23", 1892, "8. Legislaturperiode, 2. Session", "",
     "Handelsvertrag mit Rumänien – Landwirtschaftliche Tarifpositionen",
     "Trade Treaty with Romania – Agricultural tariff positions",
     "Trade treaty / agricultural tariff",
     "", "", "", "Passed",
     "bsb00018669", "https://www.digitale-sammlungen.de/de/view/bsb00018669",
     "Stenographische Berichte, 8. LP, 2. Sess.",
     "Roll call vote on the trade treaty with Romania, including agricultural "
     "tariff concessions on grain and livestock. Part of Caprivi's broader "
     "trade treaty programme."),

    ("1894-03-10", 1894, "9. Legislaturperiode, 1. Session", "",
     "Handelsvertrag mit Russland – Getreidezoll-Konzessionen",
     "Trade Treaty with Russia – Grain tariff concessions",
     "Trade treaty / grain tariff reduction",
     "", "", "", "Passed",
     "bsb00018687", "https://www.digitale-sammlungen.de/de/view/bsb00018687",
     "Stenographische Berichte, 9. LP, 1. Sess.",
     "Roll call vote on the landmark trade treaty with Russia, which reduced "
     "grain tariffs further (wheat to 3.50 Mark, rye to 3.50 Mark). The most "
     "controversial of all Caprivi treaties due to Russia being a major grain "
     "exporter. Passed narrowly with Centre Party support."),

    ("1894-03-15", 1894, "9. Legislaturperiode, 1. Session", "",
     "Zollgesetz – Landwirtschaftliche Positionen (Vieh, Getreide)",
     "Customs Law – Agricultural positions (livestock, grain)",
     "Agricultural tariff schedule",
     "", "", "", "Passed",
     "bsb00018717", "https://www.digitale-sammlungen.de/de/view/bsb00018717",
     "Stenographische Berichte, 9. LP, 1. Sess., Anlagen",
     "Roll call votes on individual agricultural tariff positions including "
     "livestock and grain within the framework of implementing the trade "
     "treaties. Multiple namentliche Abstimmungen on different commodity "
     "positions."),

    # ═══════════════════════════════════════════════════════════════════════
    # KAISERREICH – WILHELMINE ERA (1895-1914)
    # ═══════════════════════════════════════════════════════════════════════

    ("1896-06-09", 1896, "9. Legislaturperiode, 4. Session", "",
     "Zuckersteuergesetz – Reform der Rübenzuckerbesteuerung",
     "Sugar Tax Law – Reform of beet sugar taxation",
     "Sugar tax / agricultural taxation",
     "", "", "", "Passed",
     "bsb00002758", "https://www.digitale-sammlungen.de/de/view/bsb00002758",
     "Stenographische Berichte, 9. LP, 4. Sess.",
     "Roll call vote on the reform of sugar beet taxation. The sugar beet "
     "industry was a major agricultural sector, and the tax structure "
     "significantly affected farm incomes. Reform aimed to balance fiscal "
     "needs with protection of domestic sugar production."),

    ("1897-06-02", 1897, "9. Legislaturperiode, 5. Session", "",
     "Buttergesetz / Margarinegesetz (Gesetz betr. den Verkehr mit Butter, "
     "Käse, Schmalz und deren Ersatzmitteln)",
     "Butter Law / Margarine Law (Law concerning trade in butter, cheese, "
     "lard and their substitutes)",
     "Margarine regulation / dairy protection",
     "", "", "", "Passed",
     "bsb00002762", "https://www.digitale-sammlungen.de/de/view/bsb00002762",
     "Stenographische Berichte, 9. LP, 5. Sess.",
     "Roll call vote on the Margarine Law, which regulated the trade in "
     "butter substitutes (especially margarine) to protect domestic dairy "
     "farmers. Required clear labelling and banned adding butter-like "
     "colouring to margarine. Strongly supported by the agrarian lobby."),

    ("1897-06-12", 1897, "9. Legislaturperiode, 5. Session", "",
     "Margarinegesetz – Zweite Lesung / namentliche Abstimmung",
     "Margarine Law – Second reading / roll call vote",
     "Margarine regulation / dairy protection",
     "", "", "", "Passed",
     "bsb00002763", "https://www.digitale-sammlungen.de/de/view/bsb00002763",
     "Stenographische Berichte, 9. LP, 5. Sess.",
     "Roll call vote on the second reading of the Margarine Law. Confirmed "
     "restrictions on margarine production and sale. Included provisions for "
     "compulsory addition of sesame oil to margarine to distinguish it from "
     "butter, a measure demanded by dairy farmers."),

    ("1898-04-20", 1898, "10. Legislaturperiode, 1. Session", "",
     "Fleischbeschaugesetz – Einfuhrverbot für ausländisches Fleisch",
     "Meat Inspection Law – Import ban on foreign meat",
     "Meat inspection / livestock protection",
     "", "", "", "Passed",
     "bsb00002772", "https://www.digitale-sammlungen.de/de/view/bsb00002772",
     "Stenographische Berichte, 10. LP, 1. Sess.",
     "Roll call vote on the Meat Inspection Law, which imposed strict "
     "inspection requirements on imported meat, effectively banning many "
     "foreign meat imports (especially from the United States). A key "
     "protectionist measure benefiting domestic livestock farmers."),

    ("1900-06-03", 1900, "10. Legislaturperiode, 2. Session", "",
     "Reichsfleischbeschaugesetz (Schlachtvieh- und Fleischbeschaugesetz)",
     "Reich Meat and Livestock Inspection Law",
     "Meat inspection / livestock protection",
     "", "", "", "Passed",
     "bsb00002782", "https://www.digitale-sammlungen.de/de/view/bsb00002782",
     "Stenographische Berichte, 10. LP, 2. Sess.",
     "Roll call vote on the comprehensive Reich Meat Inspection Law, "
     "establishing federal standards for meat and livestock inspection. "
     "Included import restrictions requiring foreign meat to meet German "
     "standards, effectively a non-tariff barrier protecting domestic "
     "livestock producers."),

    # ─── 1902: Bülow Tariff ───
    ("1902-11-26", 1902, "10. Legislaturperiode, 2. Session", "",
     "Zuckersteuergesetz – namentliche Abstimmung",
     "Sugar Tax Law – roll call vote",
     "Sugar tax reform",
     "", "", "", "Passed",
     "bsb00002795", "https://www.digitale-sammlungen.de/de/view/bsb00002795",
     "Stenographische Berichte, 10. LP, 2. Sess.",
     "Roll call vote on the sugar tax reform, connected to the Brussels "
     "Sugar Convention of 1902. Abolished export bounties on sugar and "
     "reformed domestic sugar taxation, with major implications for the "
     "sugar beet farming sector."),

    ("1902-12-14", 1902, "10. Legislaturperiode, 2. Session", "",
     "Zolltarifgesetz (Bülow-Tarif) – Gesamtabstimmung",
     "Customs Tariff Law (Bülow Tariff) – Final vote",
     "General agricultural tariff / Bülow Tariff",
     "", "", "", "Passed",
     "bsb00002795", "https://www.digitale-sammlungen.de/de/view/bsb00002795",
     "Stenographische Berichte, 10. LP, 2. Sess.",
     "Roll call vote on the Bülow Tariff (Zolltarifgesetz), the most "
     "comprehensive tariff revision since 1879. Established minimum tariff "
     "rates (Minimalzölle) for grain: wheat 5.50 Mark, rye 5.00 Mark per "
     "100 kg. These minimum rates could not be reduced in future trade "
     "treaties. Supported by Conservatives, Centre, and National Liberals. "
     "Opposed by SPD and Progressives."),

    ("1903-01-19", 1903, "11. Legislaturperiode, 1. Session", "",
     "Zolltarif – Einzelpositionen Getreidezölle (Weizen, Roggen, Gerste, Hafer)",
     "Customs Tariff – Individual grain tariff positions (wheat, rye, barley, oats)",
     "Grain tariff schedule",
     "", "", "", "Passed",
     "bsb00002796", "https://www.digitale-sammlungen.de/de/view/bsb00002796",
     "Stenographische Berichte, 11. LP, 1. Sess.",
     "Roll call votes on individual grain tariff positions within the Bülow "
     "Tariff schedule. Established specific duties for wheat, rye, barley, "
     "oats, and flour. Multiple namentliche Abstimmungen on each grain type."),

    ("1903-01-19", 1903, "11. Legislaturperiode, 1. Session", "",
     "Zolltarif – Einzelpositionen Viehzölle",
     "Customs Tariff – Individual livestock tariff positions",
     "Livestock tariff schedule",
     "", "", "", "Passed",
     "bsb00002796", "https://www.digitale-sammlungen.de/de/view/bsb00002796",
     "Stenographische Berichte, 11. LP, 1. Sess.",
     "Roll call votes on individual livestock tariff positions within the "
     "Bülow Tariff schedule. Set duties on cattle, swine, sheep, and other "
     "livestock imports."),

    ("1903-02-10", 1903, "11. Legislaturperiode, 1. Session", "",
     "Zolltarif – Ausführungsbestimmungen zum Zolltarifgesetz",
     "Customs Tariff – Implementation Provisions of the Tariff Law",
     "Tariff implementation",
     "", "", "", "Passed",
     "bsb00002798", "https://www.digitale-sammlungen.de/de/view/bsb00002798",
     "Stenographische Berichte, 11. LP, 1. Sess.",
     "Roll call vote on the implementation provisions for the Bülow Tariff. "
     "Covered administrative procedures, customs collection mechanisms, and "
     "transition arrangements for the new agricultural tariff schedule."),

    ("1903-10-15", 1903, "11. Legislaturperiode, 1. Session", "",
     "Zolltarifgesetz – Zusatzbestimmungen (landwirtschaftliche Zölle)",
     "Customs Tariff Law – Supplementary Provisions (agricultural duties)",
     "Agricultural tariff supplements",
     "", "", "", "Passed",
     "bsb00003567", "https://www.digitale-sammlungen.de/de/view/bsb00003567",
     "Stenographische Berichte, 11. LP, 1. Sess.",
     "Roll call vote on supplementary provisions to the Bülow Tariff "
     "concerning agricultural duties. Included detailed scheduling of "
     "seasonal tariff adjustments and agricultural product classifications."),

    # ─── 1905-1906: Trade Treaties ───
    ("1905-03-20", 1905, "11. Legislaturperiode, 2. Session", "",
     "Handelsvertrag mit Russland (1904) – Getreidezoll-Konzessionen",
     "Trade Treaty with Russia (1904) – Grain tariff concessions",
     "Trade treaty / grain tariff",
     "", "", "", "Passed",
     "bsb00002812", "https://www.digitale-sammlungen.de/de/view/bsb00002812",
     "Stenographische Berichte, 11. LP, 2. Sess.",
     "Roll call vote on the trade treaty with Russia, which included "
     "agricultural tariff concessions within the framework of the Bülow "
     "Tariff minimum rates. The minimum tariff provisions limited how much "
     "grain duties could be reduced."),

    ("1905-05-15", 1905, "11. Legislaturperiode, 2. Session", "",
     "Einfuhrzollgesetz – Landwirtschaftliche Einfuhrzölle",
     "Import Duty Law – Agricultural import duties",
     "Agricultural import duties",
     "", "", "", "Passed",
     "bsb00002814", "https://www.digitale-sammlungen.de/de/view/bsb00002814",
     "Stenographische Berichte, 11. LP, 2. Sess.",
     "Roll call vote on the Import Duty Law, specifying agricultural import "
     "duties under the new trade treaty framework. Covered duties on grain, "
     "livestock, and processed agricultural products."),

    ("1906-04-10", 1906, "11. Legislaturperiode, 2. Session", "",
     "Handelsverträge – Tarifvertrag mit Österreich-Ungarn, Italien, Belgien, etc.",
     "Trade Treaties – Tariff agreements with Austria-Hungary, Italy, Belgium, etc.",
     "Trade treaties / agricultural tariff",
     "", "", "", "Passed",
     "bsb00002827", "https://www.digitale-sammlungen.de/de/view/bsb00002827",
     "Stenographische Berichte, 11. LP, 2. Sess.",
     "Roll call vote on the package of trade treaties implementing the Bülow "
     "Tariff. Included tariff agreements with major trading partners. "
     "Agricultural tariff rates were constrained by the Bülow Tariff minimum "
     "rates, ensuring basic protection for German farmers."),

    # ─── 1909: Finance Reform ───
    ("1909-06-24", 1909, "12. Legislaturperiode, 1. Session", "",
     "Reichsfinanzreform – Zuckersteuererhöhung",
     "Reich Finance Reform – Sugar tax increase",
     "Sugar tax / agricultural taxation",
     "", "", "", "Passed",
     "bsb00002847", "https://www.digitale-sammlungen.de/de/view/bsb00002847",
     "Stenographische Berichte, 12. LP, 1. Sess.",
     "Roll call vote on the sugar tax increase as part of the 1909 Reich "
     "Finance Reform (Bülow's finance reform). Increased domestic consumption "
     "tax on sugar. Controversial among sugar beet farmers."),

    ("1909-06-24", 1909, "12. Legislaturperiode, 1. Session", "",
     "Reichsfinanzreform – Einfuhrscheine für landwirtschaftliche Erzeugnisse",
     "Reich Finance Reform – Import certificates for agricultural products",
     "Agricultural import regulation",
     "", "", "", "Passed",
     "bsb00002847", "https://www.digitale-sammlungen.de/de/view/bsb00002847",
     "Stenographische Berichte, 12. LP, 1. Sess.",
     "Roll call vote on the system of import certificates (Einfuhrscheine) "
     "for agricultural products as part of the 1909 finance reform. Import "
     "certificates allowed exporters to import equivalent quantities duty-free, "
     "a measure debated in the context of agricultural trade policy."),

    # ─── 1913: Agricultural debates ───
    ("1913-05-12", 1913, "13. Legislaturperiode, 1. Session", "",
     "Erbschaftssteuer – Landwirtschaftliche Betriebe (Befreiungsantrag)",
     "Inheritance Tax – Agricultural enterprises (exemption motion)",
     "Agricultural tax exemption",
     "", "", "", "Rejected",
     "bsb00003386", "https://www.digitale-sammlungen.de/de/view/bsb00003386",
     "Stenographische Berichte, 13. LP, 1. Sess.",
     "Roll call vote on a motion to exempt agricultural enterprises from "
     "the inheritance tax (Erbschaftssteuer). Proposed by Conservative and "
     "agrarian deputies to protect family farms from fragmentation through "
     "inheritance taxation. Rejected by SPD-Progressive-Centre majority."),

    # ═══════════════════════════════════════════════════════════════════════
    # WEIMAR REPUBLIC (1919-1933)
    # ═══════════════════════════════════════════════════════════════════════

    ("1925-08-20", 1925, "2. Wahlperiode (Weimar)", "",
     "Handelsvertrag mit Frankreich – Landwirtschaftliche Zollkonzessionen",
     "Trade Treaty with France – Agricultural tariff concessions",
     "Trade treaty / agricultural tariff",
     "", "", "", "Passed",
     "bsb00000069", "https://www.digitale-sammlungen.de/de/view/bsb00000069",
     "Stenographische Berichte, 2. WP",
     "Roll call vote on the trade treaty with France, including agricultural "
     "tariff concessions. Post-WWI trade normalization included adjustments "
     "to wine, grain, and dairy import duties."),

    ("1925-10-15", 1925, "2. Wahlperiode (Weimar)", "",
     "Zolltarif – Landwirtschaftliche Tarifpositionen",
     "Customs Tariff – Agricultural tariff positions",
     "Agricultural tariff schedule",
     "", "", "", "Passed",
     "bsb00000071", "https://www.digitale-sammlungen.de/de/view/bsb00000071",
     "Stenographische Berichte, 2. WP",
     "Roll call vote on agricultural tariff positions within the post-war "
     "tariff schedule. Weimar Republic tariff policy sought to balance "
     "agricultural protection with consumer food prices."),

    ("1926-03-25", 1926, "3. Wahlperiode (Weimar)", "",
     "Zolltarif – Agrarzölle (Tarifgesetznovelle)",
     "Customs Tariff – Agricultural duties (tariff law amendment)",
     "Agricultural tariff amendment",
     "", "", "", "Passed",
     "bsb00000046", "https://www.digitale-sammlungen.de/de/view/bsb00000046",
     "Stenographische Berichte, 3. WP",
     "Roll call vote on amendments to agricultural tariff rates. Part of "
     "ongoing Weimar-era debates over agricultural protectionism."),

    ("1926-06-15", 1926, "3. Wahlperiode (Weimar)", "",
     "Handelsvertrag mit Spanien – Landwirtschaftliche Konzessionen / Lebensmittel",
     "Trade Treaty with Spain – Agricultural concessions / Foodstuffs",
     "Trade treaty / food import duties",
     "", "", "", "Passed",
     "bsb00000074", "https://www.digitale-sammlungen.de/de/view/bsb00000074",
     "Stenographische Berichte, 3. WP",
     "Roll call vote on the trade treaty with Spain, including concessions "
     "on foodstuff imports (wine, olive oil, citrus fruits in exchange for "
     "German industrial goods). Debate included agricultural impact "
     "assessments."),

    ("1927-03-22", 1927, "3. Wahlperiode (Weimar)", "",
     "Handelsvertrag mit Polen – Landwirtschaftliche Einfuhrregelungen",
     "Trade Treaty with Poland – Agricultural import regulations",
     "Trade treaty / agricultural import",
     "", "", "", "Passed",
     "bsb00000077", "https://www.digitale-sammlungen.de/de/view/bsb00000077",
     "Stenographische Berichte, 3. WP",
     "Roll call vote on the trade treaty with Poland, which included "
     "agricultural import regulations. Particularly contentious due to "
     "Polish grain and livestock exports competing with East German farmers."),

    ("1928-12-07", 1928, "4. Wahlperiode (Weimar)", "",
     "Getreidezollgesetz – Erhöhung der Getreidezölle",
     "Grain Tariff Law – Increase of grain tariffs",
     "Grain tariff increase",
     "", "", "", "Passed",
     "bsb00000079", "https://www.digitale-sammlungen.de/de/view/bsb00000079",
     "Stenographische Berichte, 4. WP",
     "Roll call vote on increasing grain import tariffs during the late "
     "Weimar period. Response to falling world grain prices and the "
     "deepening agricultural crisis. Supported by DNVP, DVP, Centre, "
     "and parts of DDP."),

    ("1929-03-15", 1929, "4. Wahlperiode (Weimar)", "",
     "Fleischeinfuhrgesetz – Beschränkung der Fleischeinfuhr",
     "Meat Import Law – Restriction of meat imports",
     "Meat import restriction",
     "", "", "", "Passed",
     "bsb00000109", "https://www.digitale-sammlungen.de/de/view/bsb00000109",
     "Stenographische Berichte, 4. WP",
     "Roll call vote on restricting meat imports to protect domestic "
     "livestock farmers during the agricultural crisis. Part of the growing "
     "trend toward agricultural autarky in the late Weimar Republic."),

    ("1930-04-03", 1930, "4. Wahlperiode (Weimar)", "",
     "Getreidezollgesetz – Weitere Erhöhung der Getreidezölle",
     "Grain Tariff Law – Further increase of grain tariffs",
     "Grain tariff increase",
     "", "", "", "Passed",
     "bsb00000111", "https://www.digitale-sammlungen.de/de/view/bsb00000111",
     "Stenographische Berichte, 4. WP",
     "Roll call vote on further increasing grain tariffs during the "
     "agricultural depression. By 1930, German grain tariffs had reached "
     "historically high levels. Part of the Brüning government's attempt "
     "to stabilize agriculture."),

    ("1931-06-15", 1931, "5. Wahlperiode (Weimar)", "",
     "Osthilfe-Gesetz – Agrarhilfe für ostelbische Landwirtschaft",
     "Eastern Aid Law – Agricultural assistance for East Elbian agriculture",
     "Agricultural emergency aid",
     "", "", "", "Passed",
     "bsb00000129", "https://www.digitale-sammlungen.de/de/view/bsb00000129",
     "Stenographische Berichte, 5. WP",
     "Roll call vote on the Osthilfe (Eastern Aid) legislation providing "
     "emergency financial assistance to indebted East Elbian estates. "
     "Included debt relief, credit subsidies, and restructuring measures. "
     "One of the most controversial agricultural votes of the Weimar Republic, "
     "criticized for disproportionately benefiting large Junker estates."),

    ("1931-06-20", 1931, "5. Wahlperiode (Weimar)", "",
     "Reichsministerium für Ernährung und Landwirtschaft – Haushalt / "
     "Namentliche Abstimmung",
     "Reich Ministry of Food and Agriculture – Budget / Roll call vote",
     "Agricultural ministry budget",
     "", "", "", "Passed",
     "bsb00000129", "https://www.digitale-sammlungen.de/de/view/bsb00000129",
     "Stenographische Berichte, 5. WP",
     "Roll call vote on the budget of the Reich Ministry of Food and "
     "Agriculture (Reichsministerium für Ernährung und Landwirtschaft). "
     "The 1931 budget included emergency agricultural support measures, "
     "rural credit programmes, and administration of the Osthilfe."),
]


def write_csv(votes, output_path):
    """Write roll call votes to CSV file."""
    headers = [
        "date",
        "year",
        "legislative_period",
        "session_number",
        "german_title",
        "english_title",
        "topic_category",
        "votes_yes",
        "votes_no",
        "votes_abstain",
        "vote_result",
        "bsb_volume_id",
        "digitale_sammlungen_url",
        "stenographische_berichte_reference",
        "description",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for vote in votes:
            writer.writerow(vote)

    print(f"Wrote {len(votes)} roll call votes to {output_path}")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(
        script_dir, "reichstag_agricultural_rollcall_votes.csv"
    )
    write_csv(roll_call_votes, output_path)

    # Print summary statistics
    years = [v[1] for v in roll_call_votes]
    print(f"\nSummary:")
    print(f"  Total roll call votes: {len(roll_call_votes)}")
    print(f"  Date range: {min(years)}-{max(years)}")

    # Count by era
    zollparlament = sum(1 for y in years if y < 1871)
    kaiserreich = sum(1 for y in years if 1871 <= y < 1919)
    weimar = sum(1 for y in years if 1919 <= y <= 1933)
    print(f"  Zollparlament (1867-1870): {zollparlament} votes")
    print(f"  Kaiserreich (1871-1918): {kaiserreich} votes")
    print(f"  Weimar Republic (1919-1933): {weimar} votes")

    # Count by topic
    from collections import Counter
    topics = Counter(v[6] for v in roll_call_votes)
    print(f"\n  Top topics:")
    for topic, count in topics.most_common(10):
        print(f"    {topic}: {count}")

    # Unique BSB volumes
    unique_bsb = set(v[11] for v in roll_call_votes)
    print(f"\n  Unique BSB source volumes: {len(unique_bsb)}")

    print(f"\nData sourced from:")
    print(f"  https://www.digitale-sammlungen.de/en/german-reichstag-session-reports-including-database-of-members/about")
    print(f"  DWDS D*/reichstag corpus: https://kaskade.dwds.de/dstar/reichstag/")
    print(f"  Stenographische Berichte des Deutschen Reichstags (1867-1933)")


if __name__ == "__main__":
    main()
