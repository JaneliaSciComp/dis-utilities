''' dis_config.py
    Static configuration constants for the DIS app, loaded once from config.cfg
    (the single source of truth — NOT duplicated here).

    Why this exists: helpers that only need a config value (e.g. DO_NOT_DISPLAY,
    DOI, PMCID) shouldn't have to import the Flask `app` object just to read
    `app.config[...]`. Importing a plain constant from this module lets such
    helpers live outside dis_responder.py with no Flask coupling.

        from dis_config import DO_NOT_DISPLAY      # instead of app.config['DO_NOT_DISPLAY']

    How it loads: this mirrors Flask's app.config.from_pyfile("config.cfg") —
    config.cfg is plain Python, so we exec it and export only the UPPERCASE names
    (Flask's own rule). Both app.config and this module therefore read the exact
    same file; the values are identical by construction.

    Deliberately NOT exported (read these from app.config instead): the runtime
    counters COUNTER, ENDPOINTS, START_TIME, LAST_TRANSACTION (mutated per
    request); the Flask/deployment flags DEBUG, RUN_MODE; the unused PROXY; and
    the KEYS auth secrets. This module is for the static domain constants only.
'''

import os

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.cfg")


# config.cfg names that are NOT static domain constants, so they are deliberately
# not exported here. Runtime counters (mutated via app.config at request time),
# Flask/deployment flags, the unused PROXY, and the KEYS auth secrets — read those
# from app.config instead.
_EXCLUDED = {"COUNTER", "ENDPOINTS", "START_TIME", "LAST_TRANSACTION",
             "DEBUG", "RUN_MODE", "PROXY", "KEYS"}


def _load_constants(path):
    ''' Exec config.cfg and return its static UPPERCASE constants.
        Keyword arguments:
          path: absolute path to config.cfg
        Returns:
          dict of {NAME: value} for each UPPERCASE assignment, minus _EXCLUDED
    '''
    namespace = {}
    with open(path, encoding="utf-8") as handle:
        code = compile(handle.read(), path, "exec")
        exec(code, namespace)  # pylint: disable=exec-used  # trusted, in-repo config
    return {key: value for key, value in namespace.items()
            if key.isupper() and key not in _EXCLUDED}


# Bind each static config constant as a module-level name so callers can do
# `from dis_config import DOI` etc.
_CONSTANTS = _load_constants(CONFIG_FILE)
globals().update(_CONSTANTS)

# Names exported for `from dis_config import *` and for linters.
__all__ = sorted(_CONSTANTS)
