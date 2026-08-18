''' ack_from_pdf.py

    Extract the Acknowledgements section from a scholarly-article PDF - a local
    file or an http(s) URL - without GROBID or any external service.

    The PDF's text layer is read with pdfminer.six (pure Python, MIT-licensed); a
    URL is downloaded into memory first (via stdlib urllib) and parsed from there.
    A heading heuristic then locates an "Acknowledg(e)ment(s)" section and captures
    its text up to the next recognized section heading (References, Funding, Author
    contributions, Conflicts of interest, Supplementary information, Appendix, a
    Data-availability statement, or the next numbered heading) or the end of the
    document.

    This is deliberately a heuristic, not a trained model: it works well on
    well-structured, single-column papers and reports a confidence signal (how the
    section was terminated, its length, and any warnings) so a caller can tell a
    clean extraction from a doubtful one. Two-column layouts can interleave text
    across columns, and a scanned (image-only) PDF has no text layer to read - both
    are surfaced as warnings rather than silently returning garbage.

    The extraction logic is exposed as importable functions
    (extract_text_from_pdf, extract_acknowledgements) so it can be reused or folded
    into a library later; the CLI is a convenience wrapper.

    Examples:
      # Print the acknowledgements found in a local PDF
      ack_from_pdf.py --pdf paper.pdf

      # From a URL, and check whether a term is present
      ack_from_pdf.py --pdf https://example.org/paper.pdf --term Janelia

      # Machine-readable output
      ack_from_pdf.py --pdf paper.pdf --json
'''

__version__ = '1.1.1'

import argparse
import io
import json
import logging
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

try:
    from pdfminer.high_level import extract_text
except ImportError:
    sys.exit("pdfminer.six is required: pip install pdfminer.six")

# --pdf accepts a local path or an http(s) URL; a URL is downloaded to memory and
# parsed from there (no temp file). Kept on urllib (stdlib) rather than requests so
# the tool's only third-party dependency stays pdfminer.six. Requests carry
# browser-like headers because many publishers reject a non-browser User-Agent;
# some (JCB/Rockefeller UP and other WAF- or subscription-protected sites) block
# automated download regardless of headers - for those, save the PDF from a browser
# and pass the local file path instead.
_URL_RE = re.compile(r'^https?://', re.IGNORECASE)
_HTTP_UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/124.0 Safari/537.36')

# pylint: disable=logging-fstring-interpolation

LOGGER = logging.getLogger('ack_from_pdf')

# Optional leading section number on a heading line: "3 ", "3. ", "3.1 ", "IV. ".
_NUM = r'(?:\d+(?:\.\d+)*\.?[ \t]+|[IVXLCM]+\.[ \t]+)?'

# The acknowledgements heading itself. US/UK spelling ("acknowledgment(s)" /
# "acknowledgement(s)"), optional number, optional trailing colon. Anchored to the
# start of a line so a mid-sentence "we gratefully acknowledge ..." is not matched.
ACK_HEADING_RE = re.compile(
    r'(?m)^[ \t]*' + _NUM + r'acknowledge?ments?\b[ \t]*:?', re.IGNORECASE)

# Headings that mark the END of the acknowledgements block. A line that is (mostly)
# just one of these terminates capture.
_BOUNDARY_HEADINGS = [
    r'references?', r'bibliography', r'literature cited', r'works cited',
    r'notes and references', r'reference list',
    r"authors?['’]? contributions?", r'author information',
    r'author contributions?', r'contributions?',
    r'funding(?:[ \t]+(?:information|sources?|statement))?',
    r'financial (?:support|disclosures?)', r'grants?',
    r'conflicts?[ \t]+of[ \t]+interest', r'competing (?:financial[ \t]+)?interests?',
    r'declarations?', r'disclosures?', r'ethic(?:s|al[ \t]+\w+)?',
    r'(?:supplementary|supporting)[ \t]+(?:information|materials?|data)',
    r'appendix', r'appendices', r'consent',
    r'data(?:[ \t]+and[ \t]+code)?[ \t]+availability', r'availability[ \t]+of[ \t]+data',
    r'code[ \t]+availability', r'abbreviations?', r'orcid', r'keywords?',
]
BOUNDARY_RE = re.compile(
    r'(?m)^[ \t]*' + _NUM + r'(?:' + '|'.join(_BOUNDARY_HEADINGS) + r')\b[ \t]*:?[ \t]*$',
    re.IGNORECASE)

# A heading continuation to strip off the front of the captured body, e.g.
# "Acknowledgements and Funding" leaves "and Funding" before the real text.
_LEADING_CONT_RE = re.compile(
    r'^[ \t]*(?:and|&)[ \t]+'
    r'(?:funding(?:[ \t]+\w+)?|financial[ \t]+support|disclosures?|'
    r'competing[ \t]+interests?)\b[:.\t ]*', re.IGNORECASE)

LONG_WARN = 5000   # captured text longer than this probably ran into the next section
SHORT_WARN = 20    # shorter than this is likely a false heading match

# Vocabulary a genuine acknowledgement almost always contains. Used only as a
# confidence signal: a capture with none of these (e.g. a running page header or a
# chapter title grabbed from a book-chapter PDF's interleaved text) is flagged
# low-confidence rather than returned as if it were the real section.
ACK_VOCAB_RE = re.compile(
    r'\b(thank|grateful|acknowledge|support|fund(?:ing|ed)?|grant|indebted|'
    r'assistance|financial|contribut|dedicat|fellowship)\b', re.IGNORECASE)


def _browser_headers(url):
    ''' Browser-like request headers for a PDF download. Many publishers reject a
        non-browser User-Agent; a Referer of the site's own origin also placates
        some WAFs.
        Keyword arguments:
          url: the target URL (used to derive the Referer origin)
        Returns:
          headers dict
    '''
    parts = urllib.parse.urlsplit(url)
    return {'User-Agent': _HTTP_UA,
            'Accept': 'application/pdf,text/html;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': f"{parts.scheme}://{parts.netloc}/"}


def _download_pdf(url, timeout=30):
    ''' Download a PDF from a URL into memory.
        Keyword arguments:
          url: http(s) URL
          timeout: per-request timeout in seconds
        Returns:
          the PDF bytes
        Raises:
          RuntimeError if the publisher blocked automated download (401/403)
          ValueError if the response is not a PDF (e.g. an HTML landing/error page)
    '''
    req = urllib.request.Request(url, headers=_browser_headers(url))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ctype = resp.headers.get('Content-Type', '')
            data = resp.read()
    except urllib.error.HTTPError as err:
        if err.code in (401, 403):
            raise RuntimeError(
                f"HTTP {err.code} - the publisher blocked automated download (bot "
                f"protection or a subscription paywall). Open the URL in a browser, "
                f"save the PDF, and pass the local file path to --pdf instead.") from err
        raise
    if not data.startswith(b'%PDF'):
        raise ValueError(f"URL did not return a PDF (Content-Type: {ctype or 'unknown'}, "
                         f"{len(data):,} bytes) - a landing page or error page?")
    return data


def extract_text_from_pdf(source):
    ''' Read a PDF's text layer from a local path or an http(s) URL.
        Keyword arguments:
          source: path to a PDF file, or an http(s) URL to one
        Returns:
          The extracted text (may be empty for a scanned/image-only PDF)
    '''
    if _URL_RE.match(source):
        return extract_text(io.BytesIO(_download_pdf(source))) or ''
    return extract_text(source) or ''


def _clean(raw):
    ''' Normalize a captured text block: drop a heading continuation, rejoin words
        hyphenated across a line break, and collapse all whitespace to single spaces.
        Keyword arguments:
          raw: the raw substring captured for the section
        Returns:
          Cleaned single-line string
    '''
    raw = raw.strip()
    raw = _LEADING_CONT_RE.sub('', raw)
    raw = re.sub(r'(?<=\w)-\n(?=\w)', '', raw)   # de-hyphenate line-broken words
    raw = re.sub(r'\s+', ' ', raw)
    return raw.strip()


def _score_candidate(ack, terminated, heading_text, after):
    ''' Score how section-like a captured block is, so the best acknowledgements
        heading can be chosen when several match (e.g. a mid-sentence "...,
        acknowledgements, peer review..." boilerplate line vs. the real section).
        Higher is better.
        Keyword arguments:
          ack: the cleaned captured text
          terminated: boundary heading that ended it, or 'eof'
          heading_text: the matched heading substring (to judge its casing)
          after: the character immediately following the heading match
        Returns:
          integer score
    '''
    score = 0
    # A real heading begins with a capital (or is ALL CAPS) and is not immediately
    # followed by a comma/semicolon - both signal a mid-sentence enumeration wrap
    # rather than a section title.
    first_alpha = re.search(r'[A-Za-z]', heading_text)
    if first_alpha and heading_text[first_alpha.start()].islower():
        score -= 5
    if after[:1] in (',', ';'):
        score -= 5
    if terminated != 'eof':                       # bounded by a real next heading
        score += 3
    if SHORT_WARN <= len(ack) <= LONG_WARN:       # plausible section length
        score += 3
    elif len(ack) > LONG_WARN:                    # ran into references/other sections
        score -= 2
    else:                                         # too short to be the section
        score -= 2
    if ack[:1].isalpha():                         # prose, not a stray "," or ";"
        score += 1
    # Acknowledgements almost always open with one of these.
    if re.match(r'(this (?:work|research|study)|we |the authors?|funding|financial|'
                r'supported|thanks?)\b', ack, re.IGNORECASE):
        score += 2
    return score


def extract_acknowledgements(text):
    ''' Locate and extract the acknowledgements section from article text.

        When more than one line looks like an "Acknowledgements" heading (a common
        trap: journals repeat the word in a first-page footer or an end-matter
        "Additional information" boilerplate), every candidate is scored and the
        most section-like one is chosen, rather than blindly taking the first.
        Keyword arguments:
          text: full article text (from extract_text_from_pdf)
        Returns:
          (ack_text_or_None, meta) where meta is a dict with:
            status:       'ok' | 'no-heading' | 'no-text'
            terminated_by: the boundary heading that ended capture, or 'eof'
            char_count:   length of the returned ack text
            warnings:     list of confidence caveats
    '''
    if not text or not text.strip():
        return None, {'status': 'no-text', 'terminated_by': None,
                      'char_count': 0, 'warnings': ['no extractable text (scanned PDF?)']}
    candidates = list(ACK_HEADING_RE.finditer(text))
    if not candidates:
        return None, {'status': 'no-heading', 'terminated_by': None,
                      'char_count': 0, 'warnings': ['no acknowledgements heading found']}
    scored = []
    for hmatch in candidates:
        body_start = hmatch.end()
        bmatch = BOUNDARY_RE.search(text, body_start)
        if bmatch:
            body_end = bmatch.start()
            terminated = re.sub(r'\s+', ' ', bmatch.group(0)).strip()
        else:
            body_end = len(text)
            terminated = 'eof'
        ack = _clean(text[body_start:body_end])
        score = _score_candidate(ack, terminated, hmatch.group(0),
                                 text[hmatch.end():hmatch.end() + 1])
        scored.append((score, hmatch.start(), ack, terminated))
    # Highest score wins; earliest heading breaks ties.
    scored.sort(key=lambda item: (-item[0], item[1]))
    _, _, ack, terminated = scored[0]
    warnings = []
    if len(ack) > LONG_WARN:
        warnings.append('long section - may include the following section')
    if len(ack) < SHORT_WARN:
        warnings.append('very short - heading may be a false match')
    if terminated == 'eof':
        warnings.append('no closing heading found - captured to end of document')
    if len(candidates) > 1:
        warnings.append(f'{len(candidates)} candidate headings - chose the most section-like')
    if ack and not ACK_VOCAB_RE.search(ack):
        warnings.append('does not read like an acknowledgement - low confidence')
    return ack, {'status': 'ok', 'terminated_by': terminated,
                 'char_count': len(ack), 'warnings': warnings}


def run(path, term=None):
    ''' Extract acknowledgements from one PDF and assemble a result dict.
        Keyword arguments:
          path: PDF file path or http(s) URL
          term: optional term to check for presence (case-insensitive)
        Returns:
          result dict (pdf, status, terminated_by, char_count, warnings, ack,
          and term_present when a term was given)
    '''
    try:
        text = extract_text_from_pdf(path)
    except Exception as err:
        LOGGER.error(f"Could not read {path}: {err}")
        return {'pdf': path, 'status': 'read-error', 'error': str(err),
                'ack': None, 'char_count': 0, 'warnings': [], 'terminated_by': None}
    ack, meta = extract_acknowledgements(text)
    result = {'pdf': path, 'ack': ack}
    result.update(meta)
    if term:
        result['term'] = term
        result['term_present'] = bool(ack) and term.lower() in ack.lower()
    return result


def main():
    ''' CLI entry point. '''
    parser = argparse.ArgumentParser(
        description="Extract the Acknowledgements section from a PDF (no GROBID).")
    parser.add_argument('--pdf', dest='PDF', required=True,
                        help='Path or http(s) URL to the PDF file')
    parser.add_argument('--term', dest='TERM', default=None,
                        help='Report whether this term appears in the acknowledgements')
    parser.add_argument('--json', dest='JSON', action='store_true', default=False,
                        help='Emit the full result as JSON')
    parser.add_argument('--verbose', dest='VERBOSE', action='store_true', default=False)
    parser.add_argument('--debug', dest='DEBUG', action='store_true', default=False)
    arg = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if arg.DEBUG else logging.INFO if arg.VERBOSE else logging.WARNING,
        format='%(levelname)s: %(message)s')
    result = run(arg.PDF, term=arg.TERM)
    if arg.JSON:
        print(json.dumps(result, indent=2))
        return
    if result.get('status') != 'ok':
        LOGGER.warning(f"No acknowledgements extracted ({result.get('status')}): "
                       f"{'; '.join(result.get('warnings') or []) or 'n/a'}")
        return
    if result.get('warnings'):
        LOGGER.info(f"warnings: {'; '.join(result['warnings'])}")
    LOGGER.info(f"terminated by: {result['terminated_by']} | chars: {result['char_count']}")
    if 'term_present' in result:
        LOGGER.info(f"term {result['term']!r} present: {result['term_present']}")
    print(result['ack'])


if __name__ == '__main__':
    main()
