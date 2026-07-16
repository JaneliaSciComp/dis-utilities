""" dis_license_lib.py
    Shared license-resolution helpers for DOI records -- not a standalone program.
    Used by sync_datacite_legal.py and sync_openalex.py to resolve jrc_license via
    a waterfall: DataCite rightsList -> OpenAlex -> PMC (efetch) -> Unpaywall.
"""

from dataclasses import dataclass, field
import logging
import os
import re
import time
import requests
import xmltodict
import pyalex
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

LOGGER = logging.getLogger(__name__)

EFETCH_PMC = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&rettype=full"
CC_LICENSE_RE = re.compile(r'creativecommons\.org/licenses/([a-z-]+)/([0-9.]+)', re.IGNORECASE)
CC0_RE = re.compile(r'creativecommons\.org/publicdomain/zero/([0-9.]+)', re.IGNORECASE)

_UNSET = object()
_STATE = {'unpaywall_unreachable': False, 'openalex_unreachable': False,
          'pmc_unreachable': False}
# A single OpenAlex read timeout is usually a slow query or blip, not the API
# being down, so retry a few times before tripping the run-wide circuit breaker.
OPENALEX_MAX_RETRIES = 3
OPENALEX_BACKOFF = 2

# pyalex sets no timeout on its requests.Session calls, so a network that can't
# reach api.openalex.org hangs for the OS-level TCP timeout (minutes) instead of
# failing fast. Inject a default timeout for any request that doesn't set one.
_ORIG_SESSION_REQUEST = requests.Session.request
def _request_with_default_timeout(self, method, url, **kwargs):
    kwargs.setdefault('timeout', 10)
    return _ORIG_SESSION_REQUEST(self, method, url, **kwargs)
requests.Session.request = _request_with_default_timeout

__version__ = '1.4.0'


@dataclass
class LicenseResult:  # pylint: disable=too-many-instance-attributes
    """ Result of a license resolution attempt """
    mapped: str = None
    tier: str = None
    pmc_skipped_no_id: bool = False
    pmc_429_exhausted: bool = False
    pmc_unreachable: bool = False
    unpaywall_not_indexed: bool = False
    unpaywall_unreachable: bool = False
    openalex_unreachable: bool = False
    # Raw license strings found (from PMC/Unpaywall) that aren't in the license
    # map, i.e. the "Unknown ... license" cases - so a caller can collect them
    # for triage.
    unknown_licenses: list = field(default_factory=list)


def cc_url_to_slug(url):
    """ Convert a Creative Commons license URL to the slug used in the license map
        Keyword arguments:
          url: Creative Commons license URL
        Returns:
          Slug (e.g. "cc-by-nc-4.0"), or None if the URL isn't a recognized CC license
    """
    if not url:
        return None
    match = CC_LICENSE_RE.search(url)
    if match:
        return f"cc-{match.group(1).lower()}-{match.group(2)}"
    match = CC0_RE.search(url)
    if match:
        return f"cc0-{match.group(1)}"
    return None


def get_pmc_license(pmcid):  # pylint: disable=too-many-return-statements
    """ Get the raw license string for a PMCID via NCBI E-utilities (efetch, db=pmc)
        Keyword arguments:
          pmcid: PMCID
        Returns:
          (license, rate_limited, unreachable) -- license is the raw string or None;
          rate_limited is True if PMC was still returning 429 after all retries;
          unreachable is True if PMC couldn't be reached at all (this or an earlier call)
    """
    if _STATE['pmc_unreachable']:
        return None, False, True
    # efetch (db=pmc) honors NCBI_API_KEY for a 10 req/s limit, unlike the legacy
    # PMC OAI-PMH endpoint (3 req/s, no key support) this used to call
    time.sleep(0.11)
    url = f"{EFETCH_PMC}&id={pmcid}&api_key={os.environ['NCBI_API_KEY']}"
    data = None
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=5)
            if not resp.ok:
                raise requests.HTTPError(resp.status_code, resp.text)
            data = xmltodict.parse(resp.text)
            break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
            LOGGER.warning(f"PMC unreachable, skipping it for the rest of this run: {err}")
            _STATE['pmc_unreachable'] = True
            return None, False, True
        except Exception as err:
            if '429' in str(err) and attempt < 2:
                wait = 60 * (attempt + 1)
                LOGGER.warning(f"PMC 429 for {pmcid}, retrying in {wait}s (attempt {attempt+1})")
                time.sleep(wait)
            elif '429' in str(err):
                LOGGER.warning(f"PMC 429 for {pmcid} after 3 attempts, skipping")
                return None, True, False
            elif isinstance(err, requests.exceptions.RequestException):
                # Transient network/stream error, e.g. ChunkedEncodingError
                # ("Response ended prematurely") - which is a RequestException but
                # NOT a ConnectionError/Timeout subclass, so it used to fall through
                # to the bare "raise" below and kill the whole run. A mid-stream
                # drop is usually per-request (not PMC being down), so retry this
                # PMCID and, if it keeps failing, skip just this DOI.
                if attempt < 2:
                    LOGGER.warning(f"PMC request error for {pmcid}, retrying "
                                   f"(attempt {attempt+1}): {err}")
                    time.sleep(2 * (attempt + 1))
                else:
                    LOGGER.warning(f"PMC request error for {pmcid} after 3 attempts, "
                                   f"skipping: {err}")
                    return None, False, True
            else:
                raise
    if not data:
        return None, False, False
    try:
        permissions = data['pmc-articleset']['article']['front']['article-meta']['permissions']
        license_url = permissions['license']['ali:license_ref']['#text']
    except (KeyError, TypeError):
        return None, False, False
    return cc_url_to_slug(license_url) or license_url, False, False


def get_unpaywall_license(doi):  # pylint: disable=too-many-return-statements
    """ Get the raw license string for a DOI from Unpaywall
        Keyword arguments:
          doi: DOI
        Returns:
          (license, not_indexed, unreachable) -- license is the raw string or None;
          not_indexed is True if Unpaywall has no record for this DOI; unreachable
          is True if Unpaywall couldn't be reached at all (this or an earlier call)
    """
    if _STATE['unpaywall_unreachable']:
        return None, False, True
    try:
        data = DL.get_doi_record(doi, source='unpaywall')
    except ValueError:
        # Unpaywall 404s with an HTML (non-JSON) body for DOIs it doesn't index
        # (e.g. datasets/software) -- expected for most DataCite DOIs, not an error
        return None, True, False
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
        LOGGER.warning(f"Unpaywall unreachable, skipping it for the rest of this run: {err}")
        _STATE['unpaywall_unreachable'] = True
        return None, False, True
    except Exception as err:
        LOGGER.warning(f"Could not get Unpaywall license for {doi}: {err}")
        return None, False, False
    if not data:
        return None, False, False
    best = data.get('best_oa_location') or {}
    if best.get('license'):
        return best['license'], False, False
    for loc in data.get('oa_locations', []):
        if loc.get('license'):
            return loc['license'], False, False
    return None, False, False


def get_openalex_record(doi):
    """ Get the OpenAlex work record for a DOI, with a circuit breaker for
        connection-level failures. doi_common's own OpenAlex handling already
        swallows all exceptions (including connection failures) and returns None,
        so it can't distinguish "not found" from "unreachable" -- call pyalex
        directly here so we can tell the difference.
        Keyword arguments:
          doi: DOI
        Returns:
          (data, unreachable) -- data is the OpenAlex work record or None;
          unreachable is True if OpenAlex couldn't be reached at all
    """
    if _STATE['openalex_unreachable']:
        return None, True
    for attempt in range(1, OPENALEX_MAX_RETRIES + 1):
        try:
            results = pyalex.Works().filter(doi=doi).get()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
            # Retry a connection-level failure a few times; only trip the run-wide
            # circuit breaker once retries are exhausted (a genuine outage will
            # still fail every attempt, but one slow response won't disable
            # OpenAlex for the rest of the run).
            if attempt < OPENALEX_MAX_RETRIES:
                LOGGER.warning(f"OpenAlex timed out for {doi}, retrying "
                               f"(attempt {attempt}/{OPENALEX_MAX_RETRIES}): {err}")
                time.sleep(OPENALEX_BACKOFF * attempt)
                continue
            LOGGER.warning(f"OpenAlex unreachable after {OPENALEX_MAX_RETRIES} attempts, "
                           f"skipping it for the rest of this run: {err}")
            _STATE['openalex_unreachable'] = True
            return None, True
        except Exception as err:
            LOGGER.warning(f"OpenAlex unavailable for {doi}: {err}")
            return None, False
        if not results:
            return None, False
        return results[0], False
    return None, True


def license_from_rights_list(row, licmap):
    """ Try to resolve a license from a DataCite rightsList
        Keyword arguments:
          row: DOI record
          licmap: license map dictionary
        Returns:
          Mapped license, or None
    """
    if not (row.get('jrc_obtained_from') == 'DataCite' and row.get('rightsList')):
        return None
    licmap_norm = {key.strip().lower(): val for key, val in licmap.items()}
    for right in row['rightsList']:
        if 'rightsIdentifier' in right and right['rightsIdentifier'] in licmap:
            return licmap[right['rightsIdentifier']]
        if right.get('rights'):
            if right['rights'] in licmap:
                return licmap[right['rights']]
            if right['rights'].strip().lower() in licmap_norm:
                return licmap_norm[right['rights'].strip().lower()]
        if 'rightsUri' in right and right['rightsUri'] in licmap:
            return licmap[right['rightsUri']]
        if 'rightsIdentifier' in right:
            LOGGER.error(f"Unknown license (rightsIdentifier) {right['rightsIdentifier']} "
                         f"for {row['doi']}")
        elif right.get('rights'):
            LOGGER.error(f"Unknown license (rights) {right['rights']} for {row['doi']}")
        elif 'rightsUri' in right:
            LOGGER.error(f"Unknown license (rightsUri) {right['rightsUri']} for {row['doi']}")
        else:
            LOGGER.error(f"Incorrect rights format {right} for {row['doi']}")
    return None


def license_from_openalex(openalex_data, licmap, doi=None):
    """ Try to resolve a license from an already-fetched OpenAlex record
        Keyword arguments:
          openalex_data: OpenAlex work record (as returned by doi_common), or None
          licmap: license map dictionary
          doi: DOI, for logging an unmapped license value
        Returns:
          Mapped license, or None
    """
    if not openalex_data:
        return None
    lic = (openalex_data.get('primary_location') or {}).get('license')
    if not lic or lic == "False":
        return None
    if lic in licmap:
        return licmap[lic]
    LOGGER.warning(f"Unknown OpenAlex license {lic} for {doi}")
    return None


def resolve_license(row, licmap, openalex_data=_UNSET):
    # pylint: disable=too-many-return-statements,too-many-branches
    """ Resolve a license for a DOI record via the full waterfall:
        DataCite rightsList -> OpenAlex -> PMC -> Unpaywall
        Keyword arguments:
          row: DOI record
          licmap: license map dictionary
          openalex_data: pre-fetched OpenAlex record. Pass None if OpenAlex has
                         already been checked and had nothing; omit entirely to
                         have this function fetch OpenAlex itself.
        Returns:
          LicenseResult
    """
    result = LicenseResult()
    lic = license_from_rights_list(row, licmap)
    if lic:
        LOGGER.info(f"Using license (rightsList) {lic} for {row['doi']}")
        result.mapped = lic
        result.tier = 'rightsList'
        return result
    if openalex_data is _UNSET:
        time.sleep(.5)
        openalex_data, openalex_unreachable = get_openalex_record(row['doi'])
        result.openalex_unreachable = openalex_unreachable
    lic = license_from_openalex(openalex_data, licmap, doi=row['doi'])
    if lic:
        LOGGER.info(f"Using license (OpenAlex) {lic} for {row['doi']}")
        result.mapped = lic
        result.tier = 'openalex'
        return result
    if row.get('jrc_pmc'):
        raw, rate_limited, pmc_unreachable = get_pmc_license(row['jrc_pmc'])
        result.pmc_429_exhausted = rate_limited
        result.pmc_unreachable = pmc_unreachable
        if raw:
            if raw in licmap:
                result.mapped = licmap[raw]
                result.tier = 'pmc'
                LOGGER.info(f"Using license (PMC) {result.mapped} for {row['doi']}")
                return result
            result.unknown_licenses.append(raw)
            LOGGER.warning(f"Unknown PMC license {raw} for {row['doi']}")
    else:
        result.pmc_skipped_no_id = True
    raw, not_indexed, unreachable = get_unpaywall_license(row['doi'])
    result.unpaywall_not_indexed = not_indexed
    result.unpaywall_unreachable = unreachable
    if raw:
        if raw in licmap:
            result.mapped = licmap[raw]
            result.tier = 'unpaywall'
            LOGGER.info(f"Using license (Unpaywall) {result.mapped} for {row['doi']}")
            return result
        result.unknown_licenses.append(raw)
        LOGGER.warning(f"Unknown Unpaywall license {raw} for {row['doi']}")
    return result
