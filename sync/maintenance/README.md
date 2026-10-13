# dis-utilities:sync/maintenance

## Periodic maintenance of the dis database

These programs correct and enrich data that is already in the `dis` database.
They are not part of the nightly synchronization: nothing upstream changes
because of them, and skipping a run costs nothing. Run them when the data
needs it, either by hand or on a long interval.

Each one proposes changes and writes nothing without `--write`.

### The orcid collection

| Name                       | Description                                                       |
| -------------------------- | ----------------------------------------------------------------- |
| add_orcid_name_variants.py | Add name variants that orcid.org holds but the collection does not |
| fix_middle_names.py        | Expand given names so every middle initial is held both ways       |
| dis_name_lib.py            | Shared name rules (splitting, initials, Latin-script test)         |
| dis_review_lib.py          | Shared interactive reviewer used by both programs                  |

Run `add_orcid_name_variants.py` first to bring in the variants ORCID knows
about, then `fix_middle_names.py` to permute what is there.

### The dois collection

| Name                        | Description                                                  |
| --------------------------- | ------------------------------------------------------------ |
| backfill_openalex_impact.py | Populate `jrc_openalex_impact` on articles and preprints      |

`backfill_openalex_impact.py` is a one-shot catch-up, not a recurring job.
`sync_citations.py` collects the same field as a side effect of its OpenAlex
lookup and writes it whenever it writes the other citation fields - including
on the paths where it skips pagination - so newly ingested DOIs are covered
without running this again.

It exists because backfilling *through* the sync is prohibitive: a
`sync_citations` pass spends roughly 33 seconds per DOI paginating
ScholeXplorer for citing DOIs, and this field comes from neither ScholeXplorer
nor the pagination. One OpenAlex work lookup takes about 0.7 seconds.

A re-run is almost always wasted. The DOIs it leaves without the field are not
failures: they are works OpenAlex holds but has never *ranked*, mostly
preprints and lightly-cited papers, and it will keep returning nothing for
them. Re-run only to retry genuine lookup errors, or with `--all` to
re-baseline existing values - unlike a citation count, a percentile can
legitimately fall, so it is deliberately kept out of the high-water-mark logic.

Delete it once the corpus is covered.

Unlike the programs above it imports from `sync/bin`, via a `sys.path` line at
the top. That is on purpose: the OpenAlex contact address, API key handling and
backoff are shared with the nightly sync rather than copied, so the two cannot
drift.

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
