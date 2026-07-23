''' seed_search_regex.py

Seed the `search_regex` MongoDB collection from the ENTITIES table below. ENTITIES is
the editable, version-controlled source of the entity-matching patterns (one key plus
a list of regex patterns each); this loader collapses each entry into a single
alternation regex - (?:pat1|pat2|...) - and upserts it as one document
{key, regex, description}. search_regex is the single runtime source of truth, read by
both tag_janelia_acks.py (the tagger) and the UI's /acksregexui. To change the patterns,
edit ENTITIES here and re-run this loader.

The "Visiting Scientist Program" key is intentionally NOT written here: it was seeded
once from dis.json's ack_search_regex (which carries a richer description), so this
loader skips it to avoid clobbering that entry.

Safety: before writing anything, the loader verifies that each combined regex matches
exactly the same acknowledgement records as its original pattern list (over every
gated record in the database). If any combined regex diverges, it aborts without
writing. Upserts are keyed on `key`, so the loader is idempotent and re-runnable.

Run (read DIS_MONGO_URI from the environment):
    python seed_search_regex.py            # verify + write
    python seed_search_regex.py --dry-run  # verify + preview only, no writes
'''

import argparse
import os
import re
import sys
import pymongo
import tag_janelia_acks as T

# Each entry: (canonical_name, [regex_patterns])
# Patterns are matched case-insensitively against the joined ack text.
# Order matters: more specific entries should appear before broader ones.
ENTITIES = [
    # ── Shared facilities ─────────────────────────────────────────────────────
    ("Advanced Imaging Center", [
        r"Advanced\s+Imaging\s+Center",
        r"\bAIC\s+Janelia\b",
        r"\bAIC\b.*?\(HHMI",
    ]),
    ("Cryo-EM Facility", [
        # Janelia before Cryo-EM
        r"(?:HHMI\s+)?(?:Janelia\s+)?CryoE[TM]\s+(?:Shared\s+Resource|Facility|Center|team|staff)",
        r"(?:HHMI\s+)?(?:Janelia\s+)?Cryo[- ‐]EM\s+"
        r"(?:Shared\s+Resource|Facility|Center|team|staff)",
        r"Janelia\s+CryoE[TM]\b",
        r"Janelia\s+Cryo[- ‐]?EM\b",
        r"Cryo[- ‐]Electron\s+Microscopy\s+(?:facility|Facility|team|Team)",
        # An explicit "Cryo" + EM term co-occurring with Janelia in the same
        # clause (bounded by '.' or ';', not just a character count). "Cryo"
        # must be literally present - a bare "EM facility"/"Electron Microscopy
        # Core" mention doesn't imply cryo-EM specifically (could be room-temp
        # or negative-stain EM, here or elsewhere), so those are intentionally
        # NOT matched on their own. No reliance on staff names (people leave,
        # and common surname+initial combos collide with people who aren't at
        # Janelia at all) or equipment brand names (e.g. "Krios" is sold to and
        # used by many other institutions and says nothing about Janelia
        # specifically). The negative lookahead excludes a clause that credits
        # Janelia as the technique's historical origin ("MicroED was developed
        # at the Janelia Research Campus...") rather than as a facility that
        # served this particular study.
        r"(?:Cryo[- ‐]?EM|CryoEM|CryoET)\b(?![^.;]*?\b(?:develop\w*|invent\w*|pioneer\w*|originat\w*|creat\w*)\b)"
        r"[^.;]*(?:at\s+|of\s+)?(?:the\s+)?(?:HHMI\s+)?Janelia",
        r"(?:HHMI\s+)?Janelia(?![^.;]*?\b(?:develop\w*|invent\w*|pioneer\w*|originat\w*|creat\w*)\b)"
        r"[^.;]*(?:Cryo[- ‐]?EM|CryoEM|CryoET)\b",
    ]),
    ("Gene Targeting and Transgenic Facility", [
        r"Gene\s+Targeting\s+(?:and|&)\s+Transgen(?:ic|ics)\s+(?:Facility|Facilities|Team|Resources|[Cc]ore)",
        r"(?:Janelia|HHMI).*?Transgenic\s+(?:Core|Resources?|Services?)\s+(?:Facility|for|[A-Z])",
        r"Transgenic\s+(?:Core\s+)?(?:Facility|Team|Services?).*?(?:Janelia|HHMI)",
        r"\bCaiying\s+Guo\b.*?(?:Janelia|HHMI)",  # heads the facility
    ]),
    ("Instrument Design and Fabrication", [
        r"Janelia\s+Instrument\s+Design\s+and\s+Fabrication",
        r"ID&F",
    ]),
    ("Invertebrate Shared Research", [
        r"Invertebrate\s+Shared\s+Research",
    ]),
    ("Project Technical Resources", [
        r"Project\s+Technical\s+Resources",
    ]),
    ("Quantitative Genomics Core", [
        r"Quantitative\s+Genomics\s+(?:Core|Resource)",
    ]),
    ("Viral Tools", [
        r"(?:Janelia Viral Core|Viral Tools core laboratory \(HHMI Janelia|Viral Tools Core Facility|Janelia Viral Tools facility|Janelia Viral Tools Facility|Janelia Farm Viral Core facility)",
    ]),
    # ── Named projects ────────────────────────────────────────────────────────
    ("FlyEM Project", [
        r"\bFlyEM\b",
        r"\bFly\s+EM\b",
        r"Janelia\s+and\s+Cambridge\s+groups",  # MANC/FANC connectome
    ]),
    ("FlyLight Project", [
        r"\bFlyLight\b",
        r"\bFly\s*Light\b",
    ]),
    ("GENIE Project", [
        # ")?" tolerates the acronym in parens, e.g. "...Effector (GENIE) Project"
        r"\bGENIE\)?\s+(?:Project|Program|[Tt]eam)",
        r"\bGINIE\s+(?:Project|Program)",
        # "[-\s]+" (not a bare "[-\s]") tolerates a hyphen-and-space typo like
        # "Genetically- Encoded", not just a lone hyphen or lone space
        r"Genetically[-\s]+Encoded\s+Neuronal\s+Indicator[s]?\s+and\s+Effector",
        r"Genetically[-\s]+Encoded\s+(?:Calcium\s+Indicator|Neuronal\s+Indicator)",
        r"\bGECI\s+[Pp]roject\b",
    ]),
    ("MouseLight Project", [
        r"\bMouseLight\b",
    ]),
    ("Open Chemistry", [
        r"Open\s+(?:the\s+)?Chemistry\s+(?:[Tt]eam|[Gg]roup)",
    ]),
    # ── Fly resources ─────────────────────────────────────────────────────────
    ("Fly Core", [
        r"Janelia\s+(?:Research\s+Campus\s+)?(?:Fly\s+Core|Fly\s+[Ff]acilit|Fly\s+Bank)",
        r"Janelia\s+fly\s+(?:stocks|lines|strains|line\s+project|[Ff]acility|[Cc]ore)",
        r"Janelia\s+(?:Research\s+(?:Campus|Center)|Farm(?:\s+Research\s+Campus)?)\s.*?\bfly\s+(?:stocks|strains|lines)\b",
        r"Janelia\s+(?:Research\s+Campus|consortium|Research\s+Center)\s+for\s+(?:providing\s+)?(?:the\s+)?(?:fly\s+)?(?:split|stocks|lines|strains|GAL4|Gal4)\b",
        r"Janelia.*?for\s+(?:providing\s+)?(?:fly\s+)?(?:stocks|lines|strains)\b",
        r"Janelia\s+(?:Gal4|GAL4)\s+fly\s+lines",
        r"Janelia.*?\band\s+the\s+(?:Vienna|Bloomington).*?fly",
        r"(?:Vienna|Bloomington).*?Janelia.*?fly",
        r"Janelia\s+(?:Farms?|Research\s+Campus).*?transgenic\s+fly",
        r"transgenic\s+fly\s+stocks.*?Janelia",
        r"Janelia\s+consortium\s+for\s+flies",
        r"Janelia.*?for\s+(?:fly\s+)?(?:antibodies\s+and\s+fly\s+stocks|gifts\s+of.*?fly)",
        r"Doe\s+lab\s+and\s+Janelia\s+for\s+gifts",
    ]),
    # ── Reagent programs ──────────────────────────────────────────────────────
    ("Materials", [
        r"Janelia\s+Materials(?:\s+Program|\s+Project)?",
    ]),
    # ── Visiting / training programs ──────────────────────────────────────────
    ("Visiting Scientist Program", [
        r"(?:\bVisiting\s+(?:Scientist|Researcher)s?\s+Program|(?:Janelia\s+(?:Farm\s+)?)?\bVisitor\s+Program(?:\s+at\s+Janelia)?)",
    ]),
    # ── Individual PI labs ───────────────────────────────────────────────────
    ("Ahrens Lab", [
        r"\bMisha\s+Ahrens\b",
        r"\bM\.?\s*Ahrens\b.*?\((?:HHMI|Janelia)",
        r"\bAhrens\b.*?Janelia\s+(?:Farm|Research)",
    ]),
    ("Betzig Lab", [
        r"\bEric\s+Betzig\b.*?(?:HHMI|Janelia)",
        r"\bBetzig\b.*?(?:HHMI|Janelia)",
    ]),
    ("Branson Lab", [
        r"\bKristin\s+Branson\b.*?(?:Janelia|HHMI)",
    ]),
    ("Dickson Lab", [
        r"\bBarry\s+Dickson\b.*?(?:HHMI|Janelia)",
        r"\bDickson\b.*?\((?:HHMI\s+)?Janelia",
        r"\bDickson\b.*?\(Janelia",
    ]),
    ("Funke Lab", [
        r"\bJan\s+Funke\b.*?(?:Janelia|HHMI)",
    ]),
    ("Harris Lab", [
        r"\bTim\s+(?:J\.?\s*)?Harris\b.*?(?:HHMI|Janelia)",
        r"\bHarris\b.*?\(HHMI\s+Janelia",
    ]),
    ("Heberlein Lab", [
        r"\bUlrike\s+Heberlein\b",
        r"\bHeberlein\b.*?\((?:HHMI,?\s+)?Janelia",
    ]),
    ("Hess Lab", [
        r"\bH[ae]r[ao]ld\s+Hess\b.*?(?:Janelia|HHMI)",  # Harald / Herald / Harald
        r"\bHess\b.*?\((?:both\s+are\s+from\s+)?Janelia",
        r"\bHess\b.*?Janelia\s+Farm",
    ]),
    ("Jayaraman Lab", [
        r"\bVivek\s+Jayaraman\b",
    ]),
    ("Lavis Lab", [
        r"\bLuke\s+(?:D\.?\s*)?Lavis\b",
        r"\bL\.?\s*D\.?\s*Lavis\b",
        r"\bL\.\s+Lavis\b",
        r"\bLuke\s+Levis\b",
        r"\bLavis\s+(?:[Ll]ab|[Ll]aboratory|[Gg]roup|[Tt]eam)\b",
        r"\bLavi.?s\s+(?:[Ll]ab|[Ll]aboratory)\b",   # covers "Lavi's", "Lavis"
        r"\bLavis\s*\(Janelia",
        r"\bLavis\s*\(HHMI",
        r"Janelia\s+Fluor[s]?",
        r"\bJaneliaFluor[s]?\b",
        r"Janelia\s+(?:Farms?|Research\s+Campus).*?\bHaloTag\s+ligand",
    ]),
    ("Lippincott-Schwartz Lab", [
        r"\bJennifer\s+Lippincott[- –]Schwartz\b",
        r"\bLippincott[- –]Schwartz\b.*?(?:Janelia|HHMI)",
    ]),
    ("Liu Lab", [
        r"\bZhe\s+Liu\b.*?Janelia",
    ]),
    ("Looger Lab", [
        r"\bLoren\s+Looger\b",
        r"\bL\.?\s*L\.?\s*Looger\b",
        r"\bLooger\b.*?(?:Janelia|HHMI)",
    ]),
    ("Reiser Lab", [
        r"\bMichael\s+Reiser\b.*?(?:Janelia|HHMI)",
        r"\bReiser\s+(?:[Ll]ab|[Gg]roup)\b.*?(?:Janelia|HHMI)",
        r"\bM\.\s+Reiser\b.*?(?:HHMI\s+)?Janelia",
    ]),
    ("Rubin Lab", [
        r"\b(?:Gerald|Gerry|G\.)\s+(?:M\.?\s+)?Rubin\b.*?(?:Janelia|HHMI)",
        r"\bRubin\b.*?(?:HHMI.*?)?/?Janelia",
        r"\bRubin\b.*?\((?:HHMI.*?)?Janelia",
    ]),
    ("Saalfeld Lab", [
        r"\bStephan\s+Saalfeld\b.*?(?:Janelia|HHMI)",
    ]),
    ("Shroff Lab", [
        r"\bHari\s+Shroff\b",
        r"\bShroff\b.*?\((?:Janelia|HHMI[- ]Janelia)",
        r"\bShroff\b.*?currently\s+at\s+Janelia",
    ]),
    ("Spruston Lab", [
        r"\bNelson\s+Spruston\b.*?(?:Janelia|HHMI)",
        r"\bSpruston\b.*?\(HHMI[- ]Janelia",
    ]),
    ("Stern Lab", [
        r"\bDavid\s+(?:L\.?\s+)?Stern\b.*?(?:Janelia|HHMI)",
        r"\bD\.?\s*(?:L\.?\s+)?Stern\b.*?\((?:HHMI[''s]*\s+)?Janelia",
    ]),
    ("Sternson Lab", [
        r"\bScott\s+Sternson\b.*?(?:Janelia|HHMI)",
        r"\bSternson\b.*?\((?:HHMI\s+)?Janelia",
        r"\bS\.\s+Sternson\b.*?(?:HHMI\s+)?Janelia",
    ]),
    ("Stringer Lab", [
        r"\bCarsen\s+Stringer\b.*?(?:Janelia|HHMI)",
    ]),
    ("Svoboda Lab", [
        r"\bKarel\s+Svoboda\b",
        r"\bSvoboda\b.*?(?:HHMI|Janelia)",
    ]),
    ("Tillberg Lab", [
        r"\bPaul\s+Tillberg\b.*?(?:Janelia|HHMI)",
    ]),
    ("Vale Lab", [
        r"\bRon(?:ald)?\s+Vale\b.*?(?:Janelia|HHMI)",
        r"\bR\.?\s+Vale\b.*?\((?:HHMI\s+)?Janelia",
    ]),
    ("Cardona Lab", [
        r"\b(?:Albert\s+)?Cardona\s+[Ll]ab\b.*?(?:Janelia|HHMI)",
        r"\bCardona\b.*?\((?:HHMI\s+)?Janelia",
    ]),
    ("Clapham Lab", [
        r"\bDavid\s+Clapham\b.*?(?:Janelia|HHMI)",
        r"\bClapham\b.*?\((?:HHMI\s+)?Janelia",
    ]),
    ("Gonen Lab", [
        r"\bTamir\s+Gonen\b.*?(?:Janelia|HHMI)",
        r"\bGonen\b.*?(?:Janelia|HHMI)",
    ]),
    ("Karpova Lab", [
        r"\b(?:Alla|Tatiana|T\.?S?\.?\s*)?Karpova\b.*?(?:Janelia|HHMI)",
    ]),
    ("Keller Lab", [
        r"\bPhilipp\s+Keller\b.*?(?:Janelia|HHMI)",
        r"\bKeller\b.*?\((?:Howard\s+Hughes|HHMI)\s+.*?Janelia",
    ]),
    ("Podgorski Lab", [
        r"\bKaspar\s+Podgorski\b.*?(?:Janelia|HHMI)",
    ]),
    ("Simpson Lab", [
        r"\bJulie\s+Simpson\b.*?(?:Janelia|HHMI)",
        r"\bSimpson\b.*?(?:HHMI\s+)?Janelia\s+Farm",
        r"\bSimpson\b.*?Janelia\s+(?:Research|Farm)",
        r"\bSimpson\b.*?\(Janelia",
    ]),
    ("Tebo Lab", [
        r"\bAlison\s+Tebo\b.*?(?:Janelia|HHMI)",
    ]),
    ("Transcription Imaging Consortium", [
        r"(?:Janelia|HHMI).*?Transcription\s+Imaging\s+Consortium",
        r"Transcription\s+Imaging\s+Consortium.*?(?:Janelia|HHMI)",
    ]),
    ("Lee Lab", [
        r"\bTzumin\s+Lee\b.*?(?:Janelia|HHMI)",
    ]),
    ("Riddiford Lab", [
        r"\bLynn\s+(?:M\.?\s+)?(?:Moorhead\s+)?Riddiford\b.*?(?:Janelia|HHMI)",
    ]),
]


# Key seeded from dis.json's ack_search_regex instead of from ENTITIES.
SKIP = {"Visiting Scientist Program"}

# Plain-english description per ENTITIES key.
DESC = {
    "Advanced Imaging Center": "The Advanced Imaging Center (AIC) at HHMI Janelia.",
    "Cryo-EM Facility": "The Janelia/HHMI Cryo-EM (CryoEM/CryoET) shared resource, or "
                        "cryo-EM/EM data collection, microscope operation, or microscopy "
                        "support at Janelia.",
    "Gene Targeting and Transgenic Facility": "The Gene Targeting and Transgenic "
                        "Facility/Core at Janelia/HHMI (incl. its transgenic core/services, "
                        "headed by Caiying Guo).",
    "Instrument Design and Fabrication": "The Janelia Instrument Design and Fabrication group.",
    "Invertebrate Shared Research": "Invertebrate Shared Research",
    "Project Technical Resources": "Project Technical Resources",
    "Quantitative Genomics Core": "The Quantitative Genomics Core/Resource.",
    "Viral Tools": "The Janelia/HHMI Viral Tools / Virus Tools core or facility.",
    "FlyEM Project": "The Janelia FlyEM project (incl. the Janelia/Cambridge MANC/FANC "
                     "connectome groups).",
    "FlyLight Project": "The Janelia FlyLight project.",
    "GENIE Project": "The Janelia GENIE project/program (genetically encoded neuronal/calcium "
                     "indicators; GECI).",
    "MouseLight Project": "The Janelia MouseLight project.",
    "Open Chemistry": "The Janelia Open Chemistry team/group.",
    "Fly Core": "The Janelia Fly Core/Facility/Bank, or Janelia-provided fly "
                "stocks/lines/strains/GAL4 (incl. Vienna/Bloomington collaborations).",
    "Materials": "The Janelia Materials program.",
    "Ahrens Lab": "PI Misha Ahrens / the Ahrens Lab at Janelia or HHMI.",
    "Betzig Lab": "PI Eric Betzig / the Betzig Lab at Janelia or HHMI.",
    "Branson Lab": "PI Kristin Branson / the Branson Lab at Janelia or HHMI.",
    "Dickson Lab": "PI Barry Dickson / the Dickson Lab at Janelia or HHMI.",
    "Funke Lab": "PI Jan Funke / the Funke Lab at Janelia or HHMI.",
    "Grimm Lab": "PI Jonathan B. Grimm / the Grimm Lab at Janelia.",
    "Harris Lab": "PI Tim Harris / the Harris Lab at Janelia or HHMI.",
    "Heberlein Lab": "PI Ulrike Heberlein / the Heberlein Lab at Janelia or HHMI.",
    "Hess Lab": "PI Harald Hess / the Hess Lab at Janelia or HHMI.",
    "Jayaraman Lab": "PI Vivek Jayaraman / the Jayaraman Lab at Janelia or HHMI.",
    "Kim Lab": "PI Douglas Kim / the Kim Lab at Janelia / HHMI / GENIE.",
    "Lavis Lab": "PI Luke Lavis / the Lavis Lab at Janelia or HHMI, incl. Janelia Fluor dyes "
                 "and HaloTag ligands.",
    "Lippincott-Schwartz Lab": "PI Jennifer Lippincott-Schwartz / the Lippincott-Schwartz Lab "
                 "at Janelia or HHMI.",
    "Liu Lab": "PI Zhe Liu / the Liu Lab at Janelia.",
    "Looger Lab": "PI Loren Looger / the Looger Lab at Janelia or HHMI.",
    "Marvin Lab": "PI Jonathan Marvin / the Marvin Lab at Janelia or HHMI.",
    "Moore Lab": "PI Andrew Moore / the Moore Lab at Janelia or HHMI.",
    "Polidoro Lab": "PI Peter Polidoro / the Polidoro Lab at Janelia or HHMI.",
    "Reiser Lab": "PI Michael Reiser / the Reiser Lab at Janelia or HHMI.",
    "Ritola Lab": "PI Kimberly Ritola / the Ritola Lab at Janelia or HHMI.",
    "Rubin Lab": "PI Gerald Rubin / the Rubin Lab at Janelia or HHMI.",
    "Saalfeld Lab": "PI Stephan Saalfeld / the Saalfeld Lab at Janelia or HHMI.",
    "Shroff Lab": "PI Hari Shroff / the Shroff Lab at Janelia or HHMI.",
    "Snapp Lab": "PI Erik Snapp / the Snapp Lab at Janelia or HHMI.",
    "Spruston Lab": "PI Nelson Spruston / the Spruston Lab at Janelia or HHMI.",
    "Stern Lab": "PI David Stern / the Stern Lab at Janelia or HHMI.",
    "Sternson Lab": "PI Scott Sternson / the Sternson Lab at Janelia or HHMI.",
    "Stringer Lab": "PI Carsen Stringer / the Stringer Lab at Janelia or HHMI.",
    "Svoboda Lab": "PI Karel Svoboda / the Svoboda Lab at Janelia or HHMI.",
    "Tillberg Lab": "PI Paul Tillberg / the Tillberg Lab at Janelia or HHMI.",
    "Vale Lab": "PI Ron Vale / the Vale Lab at Janelia or HHMI.",
    "Xu Lab": "PI C. Shan Xu / the Xu Lab at Janelia or HHMI.",
    "Cardona Lab": "PI Albert Cardona / the Cardona Lab at Janelia or HHMI.",
    "Clapham Lab": "PI David Clapham / the Clapham Lab at Janelia or HHMI.",
    "Gonen Lab": "PI Tamir Gonen / the Gonen Lab at Janelia or HHMI.",
    "Karpova Lab": "PI Alla Karpova / the Karpova Lab at Janelia or HHMI.",
    "Keller Lab": "PI Philipp Keller / the Keller Lab at Janelia or HHMI.",
    "Podgorski Lab": "PI Kaspar Podgorski / the Podgorski Lab at Janelia or HHMI.",
    "Simpson Lab": "PI Julie Simpson / the Simpson Lab at Janelia or HHMI.",
    "Tebo Lab": "PI Alison Tebo / the Tebo Lab at Janelia or HHMI.",
    "Transcription Imaging Consortium": "The Janelia/HHMI Transcription Imaging Consortium.",
    "Pfeiffer Lab": "PI Barret Pfeiffer / the Pfeiffer Lab at Janelia or HHMI.",
    "Lee Lab": "PI Tzumin Lee / the Lee Lab at Janelia or HHMI.",
    "Riddiford Lab": "PI Lynn Riddiford / the Riddiford Lab at Janelia or HHMI.",
}


def combine(patterns):
    ''' Collapse an ENTITIES pattern list into one alternation regex. '''
    return '(?:' + '|'.join(patterns) + ')'


def load_corpus(db):
    ''' Return the flattened text of every gated (Janelia/JFRC) acknowledgement record,
        used to verify combined-regex equivalence against the original pattern lists.
    '''
    corpus = []
    for coll in ('dois', 'external_dois'):
        for row in db[coll].find({"jrc_acknowledgements": {"$exists": True}},
                                 {"_id": 0, "jrc_acknowledgements": 1}):
            text = T.ack_to_text(row.get('jrc_acknowledgements', ''))
            if T.JANELIA_GATE.search(text):
                corpus.append(text)
    return corpus


def build_and_verify(corpus):
    ''' Build {key, regex, description} docs from ENTITIES and verify each combined
        regex matches the same records as its original pattern list. Returns the docs;
        exits non-zero if any description is missing or any regex diverges.
    '''
    missing = [name for name, _ in ENTITIES if name not in SKIP and name not in DESC]
    if missing:
        sys.exit(f"Missing descriptions for: {missing}")
    compiled_by_name = {name: [re.compile(p, re.IGNORECASE | re.DOTALL) for p in pats]
                        for name, pats in ENTITIES}
    docs = []
    mismatches = 0
    for name, patterns in ENTITIES:
        if name in SKIP or " Lab" in name:
            continue
        combined = combine(patterns)
        cre = re.compile(combined, re.IGNORECASE | re.DOTALL)
        indiv = compiled_by_name[name]
        bad = sum(1 for t in corpus
                  if bool(cre.search(t)) != any(p.search(t) for p in indiv))
        if bad:
            mismatches += 1
            print(f"  MISMATCH {name}: {bad} records differ")
        docs.append({"key": name, "regex": combined, "description": DESC[name]})
    if mismatches:
        sys.exit(f"{mismatches} combined regex(es) diverged from their pattern lists; not writing")
    return docs


def main(dry_run):
    db = pymongo.MongoClient(os.environ['DIS_MONGO_URI'])['dis']
    corpus = load_corpus(db)
    docs = build_and_verify(corpus)
    print(f"Verified {len(docs)} combined regexes against {len(corpus)} gated records "
          "(0 mismatches)")
    if dry_run:
        print("--dry-run: no writes")
        return
    written = 0
    for doc in docs:
        db.search_regex.update_one({"key": doc['key']}, {"$set": doc}, upsert=True)
        written += 1
    print(f"Upserted {written} ENTITIES keys into search_regex")
    print(f"search_regex now holds {db.search_regex.count_documents({})} documents")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Seed the search_regex collection from ENTITIES")
    PARSER.add_argument('--dry-run', dest='DRY', action='store_true', default=False,
                        help='Verify and preview only; do not write')
    ARG = PARSER.parse_args()
    main(ARG.DRY)
