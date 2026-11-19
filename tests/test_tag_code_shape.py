''' One shape for jrc_tag[].code.

    doi_common.get_supervisory_orgs answers name -> code two ways: from the HQ
    API it returns the bare code string; from the suporg collection it returns
    {code, active}. Every tag writer copies its answer straight into
    jrc_tag[].code, so the choice of call decides the shape on disk. The dict is
    the one the repository has settled on - update_tags.py, tag_janelia_acks.py
    and the convert_tag_codes.py migration all write and expect it - and
    update_dois.py was the straggler, putting a string on every author-derived
    tag it added and re-mixing the collection after each conversion.

    Nothing reads the field today (every reader keys on the tag's name), which
    is exactly why the drift went unnoticed. These tests make it a failure.
'''

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Programs that write jrc_tag entries and are still in service. The etl/bin
# loaders that also call the bare form are one-time migrations whose work is
# done, and are not listed.
TAG_WRITERS = ('sync/bin/update_dois.py', 'utility/bin/update_tags.py',
               'utility/bin/update_existing_dois.py',
               'sync/acknowledgements/bin/tag_janelia_acks.py')

BARE_CALL = re.compile(r'get_supervisory_orgs\(\s*\)')


def _source(rel):
    return (ROOT / rel).read_text(encoding='utf-8')


def test_every_tag_writer_reads_orgs_from_the_collection():
    ''' The bare call is the HQ API and the string shape. A writer may call it
        for some other reason, so the assertion is on the call that feeds the
        SUPORG lookup rather than on the file as a whole.
    '''
    for rel in TAG_WRITERS:
        src = _source(rel)
        assert not BARE_CALL.search(src), f"{rel} reads orgs from the HQ API (string codes)"
        assert re.search(r"get_supervisory_orgs\(\s*coll=DB\['dis'\]\.suporg\s*\)", src), \
            f"{rel} does not read orgs from the suporg collection"


def test_update_dois_says_why():
    ''' The two calls look interchangeable, and the next person to touch this
        line will not know they are not unless the code tells them.
    '''
    src = _source('sync/bin/update_dois.py')
    head = src[:src.index("orgs = DL.get_supervisory_orgs(coll=DB['dis'].suporg)")]
    assert 'not in the same shape' in head[-900:]


def test_the_api_schema_describes_the_dict():
    ''' /doi/authors passes a tag's code through verbatim, so the published
        schema must say what is actually there - and admit the older string form
        until the collection has been converted.
    '''
    src = _source('api/dis_responder.py')
    block = src[src.index('spec.components.schema("DoiTag"'):]
    block = block[:block.index('spec.components.schema("DoiAuthorsResult"')]
    assert '"object", "string", "null"' in block
    assert '"active": {"type": "boolean"}' in block
