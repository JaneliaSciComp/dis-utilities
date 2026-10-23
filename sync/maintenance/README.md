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

A routine re-run is wasted. The DOIs it leaves without the field are not
failures: they are works OpenAlex holds but has never *ranked*, mostly
preprints and lightly-cited papers, and it will keep returning nothing for
them. Re-run to retry genuine lookup errors, or with `--all` to re-baseline
existing values - unlike a citation count, a percentile can legitimately fall,
so it is deliberately kept out of the high-water-mark logic.

**One `--all` pass is outstanding.** Since v1.1.0 the program also records the
cohort the ratio was computed against - `subfield`, `topic` and `topic_score` -
and records written before that carry only the ratio. The nightly sync fills
these in as it revisits each DOI, so this is a catch-up rather than a
necessity:

    ./venv/bin/python backfill_openalex_impact.py --all --write

### The subscription collection

| Name                      | Description                                                       |
| ------------------------- | ----------------------------------------------------------------- |
| airtable_journal_types.py | Set `journal_type` on subscription journals from the Airtable base |

`airtable_journal_types.py` reads the Publications table, resolves each
publication's publisher and journal, and checks that every Publisher/Journal
pair has exactly one Journal Type (a pair with two is reported as an error and
the program exits non-zero; publications with a blank type are ignored). It
then sets `journal_type` (hybrid, open access, subscription, transformative,
preprint server) on each `subscription` record of type `Journal` whose title
matches an Airtable journal.

Matching is on the title alone, ignoring case, accents and punctuation. The
publisher is deliberately not compared: Airtable's "Wiley" is "John Wiley &
Sons, Ltd" in `subscription`, and "Elsevier BV" is "Cell Press" or "Academic
Press". A title Airtable lists under more than one type is skipped and
reported. `journal_type` is the HHMI payment-policy category and is unrelated
to `oa_status`, which `journals/bin/add_oa_status.py` sets by hand from an
access label (gold, hybrid, ...).

It reads `AIRTABLE_BASEID` and `AIRTABLE_TOKEN` (a personal access token with
the `data.records:read` scope) from the environment. They are never printed or
logged.

    ./venv/bin/python airtable_journal_types.py          # show what would change
    ./venv/bin/python airtable_journal_types.py --write

It is idempotent. A journal that later disappears from Airtable keeps its
last `journal_type`; nothing is ever removed. The Publications fetch is about
41,000 records and takes two to three minutes (Airtable allows five requests
per second).

### Why the cohort is stored

An FWCI of 1.56 means nothing without knowing 1.56x *what*. OpenAlex normalizes
against the subfield of a work's **primary topic**, together with its
publication year and type - so the comparison group for one paper is, say, 2020
Biophysics articles, decided per paper and never for the corpus as a whole.
Janelia's output spans 105 subfields, so a single median hides a great deal;
storing the cohort is what makes a per-subfield breakdown possible later.

`topic_score` is the confidence in that assignment, and it is kept because a low
one is exactly when a per-subfield comparison deserves suspicion: 0.96 on a
neuroscience paper is a safe bet, 0.56 on an interdisciplinary one is not.
Field and domain are omitted - both are derivable from the subfield.

The classification is OpenAlex's opinion, not a fact about the paper, and
nothing in the UI presents it as more than that.

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
