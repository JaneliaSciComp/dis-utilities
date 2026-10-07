''' dis_name_lib.py
    Shared name handling for the orcid collection. Both maintenance programs
    that touch the given/family name lists use these, so the rules for what
    counts as a Latin name, what counts as the same name, and how a full name
    splits live in one place.

    The given and family fields are lists of spelling variants, not a single
    name: DOI author matching compares publisher metadata against every entry,
    so "Gerald M. Rubin" and "Gerald M Rubin" both have to be present to match
    the publishers that use each form.
'''

__version__ = '1.0.0'

import re
import unicodedata

# Academic and professional suffixes that ORCID users append to a name. Left in
# place they turn "David E. Clapham MD, PhD" into a given name of its own.
SUFFIXES = ('phd', 'ph.d', 'ph.d.', 'md', 'm.d', 'm.d.', 'dphil', 'dvm', 'msc',
            'ms', 'ma', 'bs', 'ba', 'mph', 'jd', 'rn', 'dds', 'mba', 'bsc',
            'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv')
# A single letter, with or without its period: the middle initial in
# "Gerald M. Rubin" and the one in "Gerald M Rubin".
INITIAL = re.compile(r"[^\W\d_]\.?$", re.UNICODE)


def normalize(name):
    ''' Reduce a name to a comparison key: accents stripped, periods and runs
        of whitespace collapsed, lowercased. "Gerald M." and "gerald m" compare
        equal, so a variant that differs only in punctuation is not treated as
        a new name.
        Keyword arguments:
          name: name string
        Returns:
          Normalized string
    '''
    text = unicodedata.normalize("NFKD", (name or '').strip())
    text = ''.join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"[\s.]+", ' ', text.replace('.', ' ')).strip().lower()


def exact_key(name):
    ''' Comparison key that keeps periods. normalize() throws them away on
        purpose, so it cannot tell "Abigail R." from "Abigail R" - which is
        exactly the distinction the two stored forms exist to make. Used when
        deciding which spellings of a name are missing.
        Keyword arguments:
          name: name string
        Returns:
          Normalized string with periods intact
    '''
    text = unicodedata.normalize("NFKD", (name or '').strip())
    text = ''.join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"\s+", ' ', text).strip().lower()


def is_latin(name):
    ''' Test whether a name is written in Latin script.

        Decided per character by Unicode name, not by a codepoint cutoff: the
        accented forms are spread across several blocks, and anything that
        stops below Latin Extended Additional (U+1E00) throws away Vietnamese
        names like "Nguyen" (with its diacritics) while keeping "Losche".
        Punctuation and spaces are ignored, so hyphens, apostrophes and periods
        do not disqualify a name.
        Keyword arguments:
          name: name string
        Returns:
          True if every letter in the name is Latin
    '''
    for char in name or '':
        if char.isspace() or not char.isalpha():
            continue
        try:
            if not unicodedata.name(char).startswith('LATIN'):
                return False
        except ValueError:
            return False
    return True


def is_initial(token):
    ''' Test whether a token is a single-letter initial, with or without its
        period. Unicode-aware, so an accented initial still counts.
        Keyword arguments:
          token: one whitespace-delimited piece of a name
        Returns:
          True if the token is an initial
    '''
    return bool(INITIAL.fullmatch(token or ''))


def is_initials_only(name):
    ''' Test whether a name is nothing but initials ("W", "WL", "G. M.", "CJ").
        These match far too broadly to be useful for author matching, and a
        stray "WL" in the given list would pull in unrelated authors. Run-
        together initials carry no periods to count, so a short all-uppercase
        token is treated as initials too - which sends a genuinely all-caps
        short name to review rather than applying it, the safer error.
        Keyword arguments:
          name: name string
        Returns:
          True if every token is a single letter or short all-uppercase run
    '''
    tokens = [t for t in re.findall(r"[^\W\d_]+", name or '', re.UNICODE) if t]
    if not tokens:
        return True
    return all(len(tok) == 1 or (tok.isupper() and len(tok) <= 3) for tok in tokens)


def initial_variants(name):
    ''' Expand a given name containing middle initials into both spellings,
        "Abigail R." and "Abigail R". Publishers are inconsistent about the
        period, so holding one form and not the other loses matches. Only the
        two uniform forms are produced, not every combination - "Christopher
        K. E" is a spelling nobody uses.
        Keyword arguments:
          name: given name
        Returns:
          List of forms, the name as given first
    '''
    tokens = (name or '').split()
    # An initial in first position is a first name rendered as an initial,
    # which is not what this is for.
    if not any(is_initial(tok) for tok in tokens[1:]):
        return [name] if name else []
    dotted, bare = [], []
    for tok in tokens:
        if is_initial(tok):
            dotted.append(tok[0] + '.')
            bare.append(tok[0])
        else:
            dotted.append(tok)
            bare.append(tok)
    forms = []
    for form in (name, ' '.join(dotted), ' '.join(bare)):
        if form and form not in forms:
            forms.append(form)
    return forms


def first_name(givens):
    ''' The bare first name implied by a list of given-name variants, when the
        list has no single-word entry of its own. "Gerald M. Rubin" indexed
        only as "Gerald M." never matches a publisher that deposited "Gerald".

        Only trailing *initials* are dropped. A middle initial is an
        abbreviation and is safe to remove, but a middle name is part of the
        name: splitting "Seong ha" into "Seong", or "Mohammad Ali" into
        "Mohammad", invents a fragment that matches people it should not. That
        distinction matters most for the names least like the ones this rule
        was written against.
        Keyword arguments:
          givens: given-name variants held in the record
        Returns:
          First name string, or None when the list already has a bare one or
          no variant reduces to one safely
    '''
    for given in givens:
        tokens = given.split()
        if len(tokens) == 1 and not is_initial(tokens[0]):
            return None
    for given in givens:
        tokens = given.split()
        if len(tokens) < 2 or is_initial(tokens[0]):
            continue
        if all(is_initial(tok) for tok in tokens[1:]):
            return tokens[0]
    return None


def strip_suffixes(name):
    ''' Remove trailing academic and professional suffixes from a name.
        Keyword arguments:
          name: name string
        Returns:
          Name with suffixes removed
    '''
    text = (name or '').strip()
    while True:
        stripped = re.sub(r"[,\s]+(" + '|'.join(re.escape(s) for s in SUFFIXES) + r")\.?$",
                          '', text, flags=re.IGNORECASE)
        if stripped == text:
            return text.strip().rstrip(',').strip()
        text = stripped


def uninvert(name):
    ''' Convert "Family, Given" to "Given Family". ORCID users enter credit
        names both ways, and an inverted name splits against the family list
        only after it is turned around.
        Keyword arguments:
          name: name string
        Returns:
          Name in "Given Family" order, or the original if it has no comma
    '''
    if (name or '').count(',') != 1:
        return name
    family, given = (part.strip() for part in name.split(','))
    return f"{given} {family}" if family and given else name


def split_against_family(full, families):
    ''' Split a full name into (given, family) using a family name we already
        hold. Longest family first, so "Alves Ferreira" wins over "Ferreira".
        Compared token by token rather than by character offset: a held
        spelling can differ in length from the text it matches.
        Keyword arguments:
          full: full name string
          families: family-name variants held in the record
        Returns:
          (given, family) tuple, or (None, None) when no family name matches
    '''
    parts = (full or '').split()
    keys = [normalize(tok) for tok in parts]
    best_family, best_len = None, 0
    for family in families:
        fkeys = [normalize(tok) for tok in family.split()]
        if not fkeys or len(fkeys) > len(keys):
            continue
        if keys[len(keys) - len(fkeys):] == fkeys and len(fkeys) > best_len:
            best_family, best_len = family, len(fkeys)
    if best_family is None:
        return (None, None)
    given = ' '.join(parts[:len(parts) - best_len]).strip().rstrip(',').strip()
    return (given, best_family)


def split_against_given(full, givens):
    ''' Split a full name the other way round, matching a given name we hold
        against the front and treating what follows as a family name. This is
        what turns "Allison Sowell" into a proposed family name of "Sowell"
        rather than a nonsensical family name of "Allison Sowell" - a married
        name published under a maiden name, or the reverse.
        Keyword arguments:
          full: full name string
          givens: given-name variants held in the record
        Returns:
          Proposed family name, or None when no given name starts it
    '''
    parts = (full or '').split()
    keys = [normalize(tok) for tok in parts]
    best_len = 0
    for given in givens:
        gkeys = [normalize(tok) for tok in given.split()]
        # A given name consuming the whole string leaves no family name.
        if not gkeys or len(gkeys) >= len(keys):
            continue
        if keys[:len(gkeys)] == gkeys and len(gkeys) > best_len:
            best_len = len(gkeys)
    if not best_len:
        return None
    return ' '.join(parts[best_len:]).strip().rstrip(',').strip()
