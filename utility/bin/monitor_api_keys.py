""" monitor_api_keys.py
    Liveness monitor for the external API keys the DIS website and sync
    programs depend on.

    Each key is exercised against its provider's cheapest authenticated
    endpoint (the same endpoint + auth mechanism the DIS code uses). The result
    is classified as:
      OK            2xx - the key authenticated
      RATE-LIMITED  429 - the key is valid but throttled right now
      AUTH-FAIL     401/403 (NCBI also 400) - the key was rejected: expired,
                    revoked, or a subscription lapse -> the actionable signal
      DEGRADED      any other 4xx/5xx, timeout, or network error - the provider
                    or network hiccuped; this does NOT necessarily mean the key
                    is bad, so it never triggers an alert on its own
      MISSING       the environment variable is not set

    Most provider API keys are opaque tokens with no published expiration date;
    a live probe is the only reliable way to notice they have stopped working.
    Each run consumes one request per service.

    Exit status is non-zero when any key is AUTH-FAIL / MISSING, so the script
    can drive a cron or Jenkins alert. With
    --email an alert is also mailed to the DIS receivers.
"""

__version__ = '1.1.0'

import argparse
import concurrent.futures
import logging
import os
import sys
from time import sleep
import requests
import urllib3
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

ARG = LOGGER = None
DISCONFIG = {}
# Several probes reuse the DIS convention of verify=False against internal or
# enterprise TLS endpoints; silence the resulting warning noise.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# --debug sets the root logger to DEBUG, at which urllib3 logs full request
# URLs; the param-auth probes (NCBI, Springer) carry the key in the query
# string, so pin urllib3 to WARNING to keep keys out of the logs.
logging.getLogger("urllib3").setLevel(logging.WARNING)

# People API base (mirrors jrc_common.PEOPLE_BASE)
PEOPLE_BASE = 'https://hhmipeople-prod.azurewebsites.net/People/'

# One probe per key DIS actually uses. auth is (kind, name) where kind is
# 'bearer' (Authorization: Bearer <key>), 'header' (<name>: <key>), or 'param'
# (<name>=<key> query arg) - matched to how the DIS code authenticates.
# fail: HTTP codes meaning the key itself was rejected (default 401/403).
# body_fail: substrings that signal rejection even on a 2xx (NCBI habit).
PROBES = [
    {'service': 'OpenAlex', 'env': 'OPENALEX_API_KEY',
     'url': 'https://api.openalex.org/rate-limit',
     'auth': ('bearer', None)},
    {'service': 'Elsevier', 'env': 'ELSEVIER_API_KEY',
     'url': 'https://api.elsevier.com/content/search/sciencedirect',
     'auth': ('header', 'X-ELS-APIKey'),
     'extra_headers': {'Accept': 'application/json'},
     'params': {'query': 'janelia', 'count': 1}},
    {'service': 'Web of Science', 'env': 'WOS_API_KEY',
     'url': 'https://api.clarivate.com/apis/wos-starter/v1/documents',
     'auth': ('header', 'X-ApiKey'),
     'params': {'q': 'TS=janelia', 'limit': 1, 'page': 1}},
    {'service': 'Zenodo', 'env': 'ZENODO_API_KEY',
     'url': 'https://zenodo.org/api/deposit/depositions',
     'auth': ('bearer', None), 'params': {'size': 1}, 'fail': (401,)},
    {'service': 'Semantic Scholar', 'env': 'S2_API_KEY',
     'url': 'https://api.semanticscholar.org/graph/v1/paper/DOI:10.1038/nmeth.2019',
     'auth': ('header', 'x-api-key'), 'params': {'fields': 'citationCount'}},
    {'service': 'NCBI E-utilities', 'env': 'NCBI_API_KEY',
     'url': 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi',
     'auth': ('param', 'api_key'),
     'params': {'db': 'pubmed', 'term': 'janelia', 'retmode': 'json', 'retmax': 0},
     'fail': (400, 401, 403), 'body_fail': ['API key invalid']},
    {'service': 'Springer', 'env': 'SPRINGER_META_API_KEY',
     'url': 'https://api.springernature.com/meta/v2/json',
     'auth': ('param', 'api_key'),
     'params': {'q': 'doi:10.1038/nmeth.2019', 'p': 1}},
    {'service': 'protocols.io', 'env': 'PROTOCOLS_API_TOKEN',
     'url': 'https://www.protocols.io/api/v3/session/profile',
     'auth': ('bearer', None)},
    {'service': 'HHMI People', 'env': 'PEOPLE_API_KEY',
     'url': PEOPLE_BASE + 'Search/ByName/Smith',
     'auth': ('header', 'APIKey'),
     'extra_headers': {'Content-Type': 'application/json'}},
]



def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    ''' Load the DIS config (for email sender/receivers); email is disabled if
        it cannot be loaded.
        Keyword arguments:
          None
        Returns:
          None
    '''
    global DISCONFIG  # pylint: disable=global-statement
    try:
        DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    except Exception as err:
        LOGGER.warning(f"Could not load dis config (email disabled): {err}")
        DISCONFIG = {}


def build_request(probe, key):
    ''' Assemble the headers and query params for a probe, injecting the key
        the way the provider expects.
        Keyword arguments:
          probe: probe spec
          key: the API key value
        Returns:
          (headers, params) tuple
    '''
    headers = dict(probe.get('extra_headers', {}))
    params = dict(probe.get('params', {}))
    kind, name = probe['auth']
    if kind == 'bearer':
        headers['Authorization'] = f"Bearer {key}"
    elif kind == 'header':
        headers[name] = key
    elif kind == 'param':
        params[name] = key
    return headers, params


def classify(status, body, probe):
    ''' Map an HTTP status (and body) to a state string.
        Keyword arguments:
          status: HTTP status code, or None on a transport error
          body: response text (first chunk), or ''
          probe: probe spec (for per-service fail codes / body markers)
        Returns:
          One of OK, RATE-LIMITED, AUTH-FAIL, DEGRADED
    '''
    if status is None:
        return 'DEGRADED'
    if 200 <= status < 300:
        low = (body or '').lower()
        if any(marker.lower() in low for marker in probe.get('body_fail', [])):
            return 'AUTH-FAIL'
        return 'OK'
    if status == 429:
        return 'RATE-LIMITED'
    if status in probe.get('fail', (401, 403)):
        return 'AUTH-FAIL'
    return 'DEGRADED'


def probe_service(probe):
    ''' Run a single liveness probe.
        Keyword arguments:
          probe: probe spec
        Returns:
          Result dict (service, env, state, detail)
    '''
    rec = {'service': probe['service'], 'env': probe['env']}
    key = os.environ.get(probe['env'])
    if not key:
        rec.update(state='MISSING', detail='environment variable not set')
        return rec
    headers, params = build_request(probe, key)
    # Retry once on a DEGRADED result to ride out a transient network/service blip.
    for attempt in range(2):
        if attempt:
            sleep(1)
        try:
            resp = requests.get(probe['url'], headers=headers, params=params,
                                timeout=ARG.TIMEOUT, verify=False)
            rec['state'] = classify(resp.status_code, resp.text[:500], probe)
            rec['detail'] = f"HTTP {resp.status_code}"
        except Exception as err:
            rec.update(state='DEGRADED', detail=type(err).__name__)
        if rec['state'] != 'DEGRADED':
            break
    return rec




def render(results):
    ''' Print the probe table to stdout.
        Keyword arguments:
          results: probe result dicts
        Returns:
          None
    '''
    if results:
        print(f"\n{'SERVICE':18} {'ENV VAR':24} {'STATE':13} DETAIL")
        print('-' * 78)
        for rec in results:
            print(f"{rec['service']:18} {rec['env']:24} {rec['state']:13} {rec['detail']}")
    print()


def send_alert(results, alerts):
    ''' Email an alert summary to the DIS receivers.
        Keyword arguments:
          results: probe result dicts
          alerts: the actionable subset
        Returns:
          None
    '''
    receivers = DISCONFIG.get('dcreceivers') or DISCONFIG.get('receivers')
    if not (receivers and DISCONFIG.get('sender')):
        LOGGER.error("No sender/receivers in dis config; cannot email alert")
        return
    lines = ["The DIS API key monitor found keys that need attention:", ""]
    for rec in alerts:
        lines.append(f"  {rec.get('service', rec['env'])}: {rec['state']} - {rec['detail']}")
    lines += ["", "Full results:", ""]
    for rec in results:
        lines.append(f"  {rec['service']:18} {rec['state']:13} {rec['detail']}")
    try:
        JRC.send_email("\n".join(lines), DISCONFIG['sender'], receivers,
                       "DIS API key monitor: attention needed")
        LOGGER.info(f"Alert emailed to {receivers}")
    except Exception as err:
        LOGGER.error(f"Could not send alert email: {err}")


def processing():
    ''' Run the probes, report, and alert.
        Keyword arguments:
          None
        Returns:
          None
    '''
    probes = PROBES
    if ARG.SERVICE:
        term = ARG.SERVICE.lower()
        probes = [p for p in PROBES
                  if term in p['service'].lower() or term in p['env'].lower()]
        if not probes:
            terminate_program(f"No probe matches '{ARG.SERVICE}'")
    results = []
    if probes:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(probes)) as executor:
            results = list(executor.map(probe_service, probes))
        results.sort(key=lambda rec: rec['service'].lower())
    render(results)
    # Actionable items: a rejected or missing key. DEGRADED (a network/service
    # hiccup) is reported but never alerts.
    alerts = [r for r in results if r['state'] in ('AUTH-FAIL', 'MISSING')]
    if alerts and ARG.EMAIL:
        send_alert(results, alerts)
    if alerts:
        terminate_program(f"{len(alerts)} key(s) need attention "
                          + "(see AUTH-FAIL/MISSING above)")
    LOGGER.info("All checked keys authenticated successfully")
    terminate_program()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Liveness monitor for the DIS API keys")
    PARSER.add_argument('--service', dest='SERVICE', action='store', default='',
                        help='Only probe services whose name/env var matches this substring')
    PARSER.add_argument('--timeout', dest='TIMEOUT', action='store', type=int,
                        default=15, help='Per-probe HTTP timeout in seconds')
    PARSER.add_argument('--email', dest='EMAIL', action='store_true', default=False,
                        help='Email the DIS receivers when a key needs attention')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    LOGGER.info(f"Started run (version {__version__})")
    initialize_program()
    processing()
