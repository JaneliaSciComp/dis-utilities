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

### Setup

The virtual environment is shared with `sync/bin`:

    cd sync/maintenance/bin
    ln -s ../../bin/venv venv

### Tests

`tests/test_name_maintenance.py` covers both write paths and the name rules,
entirely offline. From the repository root:

    pytest tests/test_name_maintenance.py
