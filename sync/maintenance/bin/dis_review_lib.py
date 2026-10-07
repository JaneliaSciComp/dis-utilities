''' dis_review_lib.py
    Interactive review of proposed changes to the orcid collection.

    Both maintenance programs propose name variants that a person should look
    at before they are stored, so the walk - one proposal per screen, a
    decision for each, nothing written until the end - lives here rather than
    in either program.

    A candidate is a dict. Only name, field and value are required:

      name        person, as we currently hold them
      field       'given' or 'family' - the list the value would join
      value       the proposed name
      variants    pipe-joined spellings to store together, defaulting to value
      orcid       ORCID iD, rendered as a link
      key_field   field identifying the record for the write, default 'orcid'
      key_value   its value, default the candidate's orcid
      userId      userIdO365, rendered as a /userui link
      dois        DOI count, shown as a measure of what a variant is worth
      tier        short label shown beside the name (APPLY, REVIEW, ...)
      source      where the proposal came from
      note        why it needs a decision
      cur_given   given names we hold, comma-joined
      cur_family  family names we hold, comma-joined
      extra       list of (label, value) pairs for program-specific context
'''

__version__ = '1.0.0'

import collections
import sys

# Tier label colors, by convention: green for a proposal that stands on its
# own, amber for one needing judgment.
TIER_TONE = {'APPLY': '32', 'REVIEW': '33'}
KEYS = ("\n  [y] apply as shown   [g] → given   [f] → family   "
        "[e] edit   [n] skip   [b] back   [q] quit\n  > ")


def paint(text, *codes, no_color=False):
    ''' Wrap text in ANSI attributes, unless color is off or output is being
        redirected.
        Keyword arguments:
          text: string to wrap
          codes: ANSI codes
          no_color: suppress color
        Returns:
          Decorated string
    '''
    if no_color or not sys.stdout.isatty():
        return text
    return ''.join(f"\033[{code}m" for code in codes) + text + "\033[0m"


def candidate_forms(cand):
    ''' The spellings a candidate would store. One for most proposals, two
        when a middle initial is present.
        Keyword arguments:
          cand: candidate dict
        Returns:
          List of name strings
    '''
    return [f for f in (cand.get('variants') or cand['value']).split(' | ') if f]


def show_candidate(cand, index, total, no_color=False):
    ''' Print one proposal as a labeled block
        Keyword arguments:
          cand: candidate dict
          index: 1-based position
          total: number of candidates
          no_color: suppress color
        Returns:
          None
    '''
    def tint(text, *codes):
        return paint(text, *codes, no_color=no_color)

    tier = cand.get('tier') or ''
    tone = TIER_TONE.get(tier, '36')
    print(f"\n{tint('─' * 56, '2')} {tint(f'{index}/{total}', '1')} {tint('─' * 4, '2')}")
    head = f"  {tint(cand['name'], '1')}"
    head += ' ' * max(1, 34 - len(cand['name']))
    if tier:
        head += tint(tier, '1', tone)
    if cand.get('dois') is not None:
        count = int(cand['dois'])
        head += '   ' + tint(f"{count} DOI" + ('' if count == 1 else 's'), '2')
    print(head)
    if cand.get('orcid'):
        print(f"  {tint('ORCID', '2')}       https://orcid.org/{cand['orcid']}")
    if cand.get('userId'):
        print(f"  {tint('DIS', '2')}         "
              f"https://dis.int.janelia.org/userui/{cand['userId']}")
    print()
    print(f"  {tint('we hold', '2')}     given    {cand.get('cur_given') or '-'}")
    print(f"              family   {cand.get('cur_family') or '-'}")
    for label, value in cand.get('extra') or []:
        if value:
            print(f"  {tint(label, '2')}{' ' * max(1, 12 - len(label))}{value}")
    print()
    forms = candidate_forms(cand)
    shown = ', '.join(repr(f) for f in forms)
    print(f"  {tint('PROPOSAL', '1')}    add {tint(shown, '1', tone)}"
          f" to {tint(cand['field'], '1')}")
    if len(forms) > 1:
        print(f"              {tint('both spellings of the middle initial', '2')}")
    if cand.get('source'):
        print(f"  {tint('from', '2')}        {cand['source']}")
    if cand.get('note'):
        print(f"  {tint('note', '2')}        {cand['note']}")


def review_candidates(cands, no_color=False):
    ''' Walk the proposals one at a time, collecting a decision for each.
        Keyword arguments:
          cands: candidate dicts
          no_color: suppress color
        Returns:
          List of (orcid, field, value) tuples the reviewer accepted
    '''
    # Keyed by position rather than appended, so "back" can drop exactly the
    # decision it belongs to - a proposal may contribute two spellings, and a
    # person may appear several times in a row.
    decisions = {}
    total = len(cands)
    index = 0
    while index < total:
        cand = cands[index]
        show_candidate(cand, index + 1, total, no_color=no_color)
        try:
            choice = input(KEYS).strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nInterrupted.")
            break
        if choice in ('q', 'quit'):
            break
        if choice in ('b', 'back'):
            # Dropping the previous decision is what makes "back" useful; it is
            # the only way to undo a mis-keyed 'y' before anything is written.
            index = max(0, index - 1)
            decisions.pop(index, None)
            continue
        if choice in ('n', '', 'skip'):
            decisions.pop(index, None)
            index += 1
            continue
        values, field = candidate_forms(cand), cand['field']
        if choice in ('e', 'edit'):
            try:
                edited = input(f"  new value [{cand['value']}]: ").strip()
                field = input(f"  field [{field}]: ").strip().lower() or field
            except (EOFError, KeyboardInterrupt):
                print("\nInterrupted.")
                break
            # An edited value is taken literally - the reviewer typed the
            # spelling they want, so it is not expanded a second time.
            if edited:
                values = [edited]
        elif choice in ('g', 'given'):
            field = 'given'
        elif choice in ('f', 'family'):
            field = 'family'
        elif choice not in ('y', 'yes'):
            print(paint("  Unrecognized - use y, g, f, e, n, b or q.", '33', no_color=no_color))
            continue
        if field not in ('given', 'family'):
            print(paint(f"  '{field}' is not a field - use given or family.", '31',
                        no_color=no_color))
            continue
        # Records are not all identified by ORCID - three in the collection
        # have given names but no ORCID, and querying {'orcid': None} would
        # match every record missing the field, updating an arbitrary one.
        key = (cand.get('key_field', 'orcid'), cand.get('key_value', cand.get('orcid')))
        if key[1] is None:
            print(paint("  No identifier for this record - cannot store.", '31',
                        no_color=no_color))
            index += 1
            continue
        decisions[index] = [(key, field, value) for value in values]
        shown = ', '.join(repr(v) for v in values)
        print(paint(f"  accepted: {shown} → {field}", '32', no_color=no_color))
        index += 1
    return [entry for pos in sorted(decisions) for entry in decisions[pos]]


def summarize(accepted, no_color=False):
    ''' Print what the reviewer accepted
        Keyword arguments:
          accepted: list of (orcid, field, value) tuples
          no_color: suppress color
        Returns:
          None
    '''
    print(f"\n{'─' * 62}")
    if not accepted:
        print("No changes accepted - nothing written.")
        return
    print(paint(f"Accepted {len(accepted):,} change(s):", '1', no_color=no_color))
    for key, field, value in accepted:
        print(f"   {str(key[1]):<26} {field:<7} += {value!r}")


def apply_decisions(coll, accepted, logger):
    ''' Store accepted variants. $addToSet keeps the write idempotent and, more
        importantly, never rebuilds the array from a stale read - these lists
        are also written by apply_orcids.py and by the sibling program here.
        Keyword arguments:
          coll: orcid collection
          accepted: list of ((key_field, key_value), field, value) tuples
          logger: logger for records that did not update
        Returns:
          Number of names written
    '''
    grouped = collections.defaultdict(lambda: collections.defaultdict(list))
    for key, field, value in accepted:
        grouped[key][field].append(value)
    written = 0
    for key, fields in grouped.items():
        update = {field: {'$each': values} for field, values in fields.items()}
        result = coll.update_one({key[0]: key[1]}, {'$addToSet': update})
        if result.modified_count:
            written += sum(len(v) for v in fields.values())
        else:
            logger.warning(f"{key[1]}: nothing updated (variant may already be present)")
    return written
