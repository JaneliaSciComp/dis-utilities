''' The reflected-XSS class these tests pin down.

    Every UI route builds its page as an HTML string and hands it to a template
    that renders it with |safe. That is deliberate - the string IS the page - but
    it means any request value interpolated into it is a raw-HTML sink. Several
    dozen routes took a path segment (a year, an org, an ORCID) straight into a
    heading, a warning or an href.

    The fix has three parts, and each is pinned here, because all three are the
    kind of thing a later edit undoes without noticing:
      - Safe.__html__ makes Jinja honor the codebase's own trust marker, so a
        template can autoescape title= and message= instead of reaching for |safe
      - render_warning escapes its message and returns Safe
      - the link builders escape the URL they interpolate
'''

import re
from pathlib import Path

import pytest

import dis_html as DH


HOSTILE = "<img src=x onerror=alert(1)>"
TEMPLATES = Path(__file__).resolve().parents[1] / 'api' / 'templates'


# --- the trust marker -----------------------------------------------------

def test_safe_implements_the_escape_protocol():
    ''' The assertion that does not need Jinja installed to be worth making:
        __html__ is the whole contract, and losing it silently re-escapes every
        heading in the app.
    '''
    assert DH.safe('<i>ok</i>').__html__() == '<i>ok</i>'


def test_safe_is_honored_by_jinja():
    ''' Without __html__, Jinja cannot tell a Safe value from a plain str and
        escapes it - which is what drove the templates to |safe in the first
        place.
    '''
    jinja2 = pytest.importorskip('jinja2')
    env = jinja2.Environment(autoescape=True)
    tmpl = env.from_string('{{ v }}')
    assert tmpl.render(v=DH.safe('<i>ok</i>')) == '<i>ok</i>'
    assert tmpl.render(v=HOSTILE) == ('&lt;img src=x onerror=alert(1)&gt;')


def test_a_plain_string_is_still_escaped_in_a_table_cell():
    assert '<img' not in DH._render_cell(HOSTILE)


# --- render_warning -------------------------------------------------------

def test_render_warning_escapes_its_message():
    out = DH.render_warning(HOSTILE)
    assert '<img' not in out
    assert '&lt;img' in out


def test_render_warning_still_emits_its_own_icon_markup():
    ''' It escapes the message, not itself - the icon has to stay HTML. '''
    out = DH.render_warning("plain")
    assert "<span class='fas fa-" in out


def test_render_warning_returns_a_trusted_value():
    ''' Otherwise autoescaping templates would show the icon markup literally. '''
    assert isinstance(DH.render_warning("plain"), DH.Safe)


@pytest.mark.parametrize('severity', ['error', 'warning', 'info', 'success', 'na', 'no'])
def test_every_severity_escapes(severity):
    assert '<img' not in DH.render_warning(HOSTILE, severity)


# --- the link builders ----------------------------------------------------

def test_year_pulldown_escapes_a_hostile_prefix():
    ''' prefix carries a path segment on many pages, straight into an href. '''
    out = DH.year_pulldown(f"org_detail/{HOSTILE}")
    assert '<img' not in out


def test_year_pulldown_escapes_the_selected_label():
    out = DH.year_pulldown('dois_report', selected=HOSTILE)
    assert '<img' not in out


def test_download_button_escapes_the_filename():
    ''' The download name is built from a path segment on many pages. '''
    out = DH.create_downloadable(HOSTILE, None, 'a\tb\n')
    assert '<img' not in out
    assert 'href="/download/' in out


# --- the templates --------------------------------------------------------

@pytest.mark.parametrize('kwarg', ['title', 'message'])
def test_no_template_renders_the_injectable_kwargs_unescaped(kwarg):
    ''' title= and message= are built from request values all over the app, so
        they must go through autoescaping. html= legitimately stays |safe: it is
        the page body the view assembled.
    '''
    offenders = []
    for path in sorted(TEMPLATES.glob('*.html')):
        text = path.read_text(encoding='utf-8')
        if re.search(rf'\{{\{{\s*{kwarg}\s*\|\s*safe\s*\}}\}}', text):
            offenders.append(path.name)
    assert not offenders, f"{kwarg}|safe is a raw-HTML sink; found in {offenders}"
