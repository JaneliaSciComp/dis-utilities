# dis-utilities:sync/maintenance

## Periodic maintenance of the orcid collection

These programs correct and enrich data that is already in the `dis` database.
They are not part of the nightly synchronization: nothing upstream changes
because of them, and skipping a run costs nothing. Run them when the data
needs it, either by hand or on a long interval.

Each one proposes changes and writes nothing without `--write`.

| Name                       | Description                                                       |
| -------------------------- | ----------------------------------------------------------------- |
| add_orcid_name_variants.py | Add name variants that orcid.org holds but the collection does not |
| fix_middle_names.py        | Expand given names so every middle initial is held both ways       |
| dis_name_lib.py            | Shared name rules (splitting, initials, Latin-script test)         |
| fix_figshare_names.py      | Correct misspelled Janelia author names on figshare items          |
| dis_review_lib.py          | Shared interactive reviewer used by both programs                  |

Run `add_orcid_name_variants.py` first to bring in the variants ORCID knows
about, then `fix_middle_names.py` to permute what is there.

### Why both spellings of a middle initial

DOI author matching compares publisher metadata against each entry in a
record's `given` list, and publishers are inconsistent about the period. A name
held only as `Gerald M. Rubin` silently fails to match every publisher that
deposited `Gerald M Rubin`, so both are stored.

### Reviewing proposals

Neither program applies a name change unattended by default. `--review` walks
the proposals one at a time, showing the record and the reason, and stores only
what is accepted:

    ./venv/bin/python add_orcid_name_variants.py --review --write

`--review` needs a terminal and refuses to run from a pipe or a redirect.
Without `--review`, `--write` applies the proposals the program considers
unambiguous and prints the rest.

| Flag          | Effect                                                         |
| ------------- | --------------------------------------------------------------- |
| `--review`    | Walk proposals interactively (requires a terminal)              |
| `--write`     | Apply changes; without it the run is a dry run                  |
| `--dedupe`    | Also collapse exact duplicate entries (fix_middle_names.py)     |
| `--non-latin` | Include non-Latin-script proposals (add_orcid_name_variants.py) |
| `--tier`      | Which proposals to walk: `all`, `apply`, `review`               |
| `--file`      | TSV path; with `--review`, replay it instead of rescanning      |
| `--manifold`  | MongoDB manifold (`dev`, `prod`)                                |
| `--no-color`  | Plain output                                                    |

### A note on how these write

Given-name lists are written with `$addToSet`, never a whole-array `$set`.
`apply_orcids.py` and the sibling program here write the same lists, and
rebuilding an array from a stale read silently drops anything another process
added in between. The one write that does need the whole array - collapsing
duplicates under `--dedupe` - re-reads the record immediately beforehand.

### Correcting author names on figshare

`fix_figshare_names.py` takes the figshare subset of the name mismatches that
`/dois_name_mismatch` reports and offers to correct them at the source. An
author is credited on a DOI only when their name resolves against the orcid
collection, so a name figshare holds slightly wrong leaves real work
uncredited.

It needs a figshare token belonging to an institutional admin, read from
`FIGSHARE_JWT` or passed with `--token`.

Three things about figshare shape what this can do, and they are worth knowing
before running it with `--write`:

**An author record cannot be renamed.** figshare's authors API is read-only.
The only correction available is to point the item at a *different* author
record, so the misspelled record survives, orphaned. The program resolves the
replacement by searching for the roster spelling and never sends a bare name:
figshare creates a fresh record for every name string it is handed and does not
deduplicate them, so that would swap one orphan for another.

**Correcting an author mints a new version.** Adding, editing or removing an
author is on figshare's versioning trigger list, so publishing a corrected item
creates a new version and a new `.vN` DOI. The base DOI continues to resolve to
the latest version, but the old version DOI freezes with the wrong name, and
figshare does not remove versions. figshare can disable automatic versioning
for an institution, but only by support request. Nothing is written without
`--write` and a per-name confirmation that says so.

**Some mismatches cannot be fixed at all**, and the program reports rather than
skips them silently:

| Situation | What it means |
| --- | --- |
| registered figshare account | The name belongs to a person's profile, not to the item. Correcting it would rename them on everything they have posted. |
| already corrected in a later version | Our DOI is a version DOI whose frozen metadata still carries the typo. Old versions cannot be modified by any means. |
| no correctly-named record | No figshare author carries the roster spelling, and inventing one would create another orphan. |
| ambiguous replacement | Several records carry the name and nothing tells them apart. `--write` asks which to use. |

A base DOI and its version DOIs name one article, and the collection holds
both, so the program groups by article: one article, one correction.

### Setup

The virtual environment is shared with `sync/bin`:

    cd sync/maintenance/bin
    ln -s ../../bin/venv venv

### Tests

`tests/test_name_maintenance.py` covers both write paths and the name rules,
entirely offline. From the repository root:

    pytest tests/test_name_maintenance.py
