''' No view hands unmarked HTML to a template variable the template escapes.

    The templates autoescape {{ title }} and {{ message }} (the reflected-XSS fix), so a
    value built as a plain string that contains markup - "<span ...>", "<br>", or an
    entity such as "&middot;" - is shown to the reader as literal text. It has to be
    marked with safe() once it is finished, with anything from the URL or the database
    escaped first. This scans every render_template call in api/ for that mistake.

    The check follows a variable through its assignments in the view: the value counts
    as marked when its last assignment before the call is safe(...) (or render_warning,
    which returns a Safe value). "title += ..." after a safe() undoes it, because
    str + Safe is a plain str, and the check treats it that way.
'''

import ast
import re
from pathlib import Path

import pytest

API = Path(__file__).resolve().parents[1] / 'api'
MARKUP = re.compile(r"<\s*/?\s*[a-zA-Z][^>]*>|&[a-z]+;|&#\d+;")
MARKERS = ('safe', 'render_warning', 'Safe', 'Markup')


def _escaped_variables():
    ''' Template name -> the variables it prints with autoescaping ({{ x }}, not |safe) '''
    found = {}
    for tmpl in (API / 'templates').glob('*.html'):
        names = set()
        for match in re.finditer(r"\{\{\s*([A-Za-z_]\w*)\s*(\|[^}]*)?\}\}",
                                 tmpl.read_text(encoding='utf-8')):
            if not (match.group(2) and 'safe' in match.group(2)):
                names.add(match.group(1))
        found[tmpl.name] = names
    return found


def _marked(node):
    return isinstance(node, ast.Call) and \
        getattr(node.func, 'id', getattr(node.func, 'attr', '')) in MARKERS


def _unmarked_html(path, escaped):
    tree = ast.parse(path.read_text(encoding='utf-8'))

    def has_markup(node):
        # ast.unparse keeps literal and f-string text; get_source_segment re-splits the
        # whole file on every call, which made this test take most of a minute
        return bool(MARKUP.search(ast.unparse(node)))

    hits = []
    for fn in (n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))):
        assigns = {}
        for node in ast.walk(fn):
            if isinstance(node, (ast.Assign, ast.AugAssign)):
                for target in (node.targets if isinstance(node, ast.Assign) else [node.target]):
                    if isinstance(target, ast.Name):
                        assigns.setdefault(target.id, []).append(node)
        for chain in assigns.values():
            chain.sort(key=lambda n: n.lineno)
        for call in (n for n in ast.walk(fn) if isinstance(n, ast.Call)
                     and getattr(n.func, 'id', '') == 'render_template'):
            if not call.args or not isinstance(call.args[0], ast.Constant):
                continue
            names = escaped.get(call.args[0].value, set()) | escaped.get('base.html', set())
            for kw in call.keywords:
                if kw.arg not in names or _marked(kw.value):
                    continue
                value = kw.value
                if isinstance(value, ast.Name):
                    before = [a for a in assigns.get(value.id, []) if a.lineno < call.lineno]
                    start = max((i for i, a in enumerate(before) if isinstance(a, ast.Assign)),
                                default=0)
                    chain = before[start:]
                    if chain and isinstance(chain[-1], ast.Assign) and _marked(chain[-1].value):
                        continue
                    if any(has_markup(a.value) for a in chain):
                        hits.append(f"{path.name}:{call.lineno} {fn.name} {kw.arg}={value.id}")
                elif has_markup(value):
                    hits.append(f"{path.name}:{call.lineno} {fn.name} {kw.arg}=(inline)")
    return hits


ESCAPED = _escaped_variables()


def test_the_main_templates_escape_title_and_message():
    ''' The premise of the check: if a template stops escaping, this test should be
        revisited rather than silently passing. '''
    assert 'title' in ESCAPED['general.html']
    assert {'title', 'message'} <= ESCAPED['warning.html']
    assert {'title', 'message'} <= ESCAPED['error.html']


@pytest.mark.parametrize('module', sorted(p.name for p in API.glob('*.py')))
def test_no_unmarked_html_reaches_an_escaped_template_variable(module):
    hits = _unmarked_html(API / module, ESCAPED)
    assert not hits, "HTML would be shown as text - mark it safe() once built, " \
                     "escaping what comes from the URL or database:\n  " + "\n  ".join(hits)


def test_the_check_catches_the_patterns_it_is_for(tmp_path):
    ''' A view with each mistake this was written for, and the fixed form of each. '''
    sample = tmp_path / 'sample.py'
    sample.write_text('''
def plain_span(group):
    title = f"For <span>{group}</span>"
    return render_template('general.html', title=title)

def safe_then_append(group):
    title = safe(f"For <span>{escape(group)}</span>")
    title += " (Internal only)"
    return render_template('general.html', title=title)

def entity(tag):
    title = "DOIs"
    title += f" &middot; {tag}"
    return render_template('general.html', title=title)

def inline_message(sub):
    return render_template('warning.html', title=render_warning("x"),
                           message=f"Nothing for {sub}<br>")

def fixed(group):
    title = "For <span>" + escape(group) + "</span>"
    title += " (Internal only)"
    title = safe(title)
    return render_template('general.html', title=title)

def fixed_plain(tag):
    title = "DOIs"
    title += f" · {tag}"
    return render_template('general.html', title=title)
''', encoding='utf-8')
    hits = _unmarked_html(sample, ESCAPED)
    flagged = {h.split()[1] for h in hits}
    assert flagged == {'plain_span', 'safe_then_append', 'entity', 'inline_message'}
