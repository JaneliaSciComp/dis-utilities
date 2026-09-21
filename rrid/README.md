# RRID utilities

Tools for Research Resource Identifiers ([rrids.org](https://www.rrids.org)) — the
IDs that name the reagents, tools, organisms and core facilities a paper used.
Janelia appears in the registry three different ways, and conflating them gives a
badly wrong number, so each tool reports them separately.

| Program | What it does |
|---|---|
| `associate_rrids.py` | Associate registry RRIDs with `suporg` records, interactively |
| `janelia_rrids.py`   | List every Janelia-associated RRID in the SciCrunch registry |
| `harvest_rrids.py`   | Find RRIDs *cited by* Janelia papers, from PMC full text |

The UI counterpart is `/rrid` in the DIS app (System &rarr; External systems),
which resolves a single RRID or searches the registry by name.

## Credentials

`SCICRUNCH_API_KEY` (free, from a SciCrunch account) is needed by
`associate_rrids.py` and by `janelia_rrids.py`. The public resolver
(`scicrunch.org/resolver/RRID:<id>.json`) needs no key and answers for every
authority in one call, but only the keyed Elasticsearch API returns
`provenance.lastSeenDate`, so anything that cares about record freshness uses the
latter. `harvest_rrids.py` needs no key at all.

## Things that will bite you

- **The ID is permanent; the name is not.** Registry names are curated and drift.
  Never join on the name — cache it with a timestamp and re-resolve.
- **A retired resource still resolves normally.** The only machine-readable signal
  is the string `NO LONGER IN SERVICE` in `item.availability`. There is no boolean
  and no superseded-by pointer.
- **`distributions.deprecated` means deprecated *URLs***, not a deprecated record.
- **Registry data is dirty.** `SCR_026515` carries trailing whitespace in its
  official name; strip before matching.
- **Fuzzy scores do not separate right from wrong here.** `MouseLight Project` →
  `MouseLight` scores 71 and is correct; `Light Microscopy` → `Electron Microscopy`
  scores 74 and is wrong, because no Light Microscopy suporg exists. There is no
  safe auto-accept threshold, which is why `associate_rrids.py` confirms
  everything. Use `token_sort_ratio`: `WRatio` and `token_set_ratio` score that
  same wrong pair 86 and 100.
- **Searching the phrase "Janelia Research Campus" silently drops the largest
  pool** — the ~36,700 commercial antibodies conjugated to Janelia Fluor® say
  "Janelia Fluor", not the campus name. Search `Janelia` and classify.
- **The resolver is rate-limited** (~1 req/s serial; 8 concurrent threads 429s
  almost everything). The keyed Elastic API is not, in practice.
- **Do not widen `DL.get_supervisory_orgs()`** to return the new `rrid` field. The
  acknowledgement taggers store its whole return dict into
  `jrc_acknowledge[].code`, so widening it would stamp `rrid` across the `dois`
  collection. Add a separate accessor.

## Licensing

The SciCrunch Registry is CC-BY. Caching its metadata internally is fine;
attribute SciCrunch if it is surfaced publicly.
