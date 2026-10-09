# Restricted pages

Four pages name individual people alongside the deposits they are credited on:

| | |
|---|---|
| `/figshare_users` | `/protocolsio_users` |
| `/zenodo_users` | `/dryad_users` |

They are limited to a named list of readers. This is not secrecy — the DOIs are
public and the roster is visible elsewhere on the site — it is keeping the
convenient aggregation to the people who need it, until the application grows
real accounts. When that lands, delete the *Restricted pages* section of
`api/dis_responder.py`, the `@restricted` decorators, and this document.

## How it works

A `@restricted` decorator on the four views. Anyone without a signed-in session
is redirected to `/restricted_login`, and sent on to the page they asked for
once they sign in. One sign-in covers all four. `/restricted_logout` ends it.

The session lives in a cookie signed with `DIS_SESSION_SECRET`, so it cannot be
forged or edited — changing a byte invalidates it.

**The check is in the application, not at the edge.** Production is OpenShift,
and the route runs browser → HAProxy router → Service → gunicorn with no proxy
of ours in between. The `nginx.conf` in the repository root is left over from an
older docker-compose deployment and is not read by anything; a rule added there
would be silently dead.

## The two settings

Both are environment variables, read from the `dis-utilities-secrets` Secret
that the deployment already pulls in through `envFrom`. **There is no file to
edit.**

| | |
|---|---|
| `DIS_SESSION_SECRET` | Signs the session cookie. |
| `DIS_RESTRICTED_USERS` | Who may read, as `name:hash,name:hash`. |

`DIS_SESSION_SECRET` **must be identical in every replica.** The deployment runs
two. A browser that signs in against one pod is logged out the moment the
Service sends it to the other, which looks like a random, intermittent bug.

Passwords are never stored — only scrypt hashes, in the format the application
verifies against.

If either variable is missing, the four pages return **503** and say so. Missing
configuration denies access rather than granting it: a page that had quietly
stopped being restricted would look exactly like one that was working.

## Adding a reader

`manage_restricted_users.py` prompts for a name and password, hashes it, merges
it into the existing roster, and prints the commands that install the result. It
writes nothing and contacts no cluster, so a mistake here cannot reach
production.

```bash
# Always start from the roster you are adding to - see the warning below.
CURRENT=$(oc -n dis-utilities get secret dis-utilities-secrets \
          -o jsonpath='{.data.DIS_RESTRICTED_USERS}' | base64 -d)

python3 utility/bin/manage_restricted_users.py --user jane.doe --current "$CURRENT"
```

It prints an `oc set data ...` command and an `oc rollout restart`. Run them.

> **Check the header line before you install anything.** It names everyone who
> will be in the roster:
>
> ```
> DIS_RESTRICTED_USERS now holds 4 reader(s): alice, bob, carol, jane.doe
> ```
>
> If a name you expect is missing, stop. Without `--current` (and with nothing
> in your environment) the tool starts from an empty roster, so the value it
> prints holds **only the person you just added** — installing that removes
> everybody else, and nothing complains, because a one-person roster is
> perfectly valid.

### Other operations

```bash
manage_restricted_users.py --list                     # who can read right now
manage_restricted_users.py --user NAME                # add (prompts for the name if omitted)
manage_restricted_users.py --remove NAME              # take access away
manage_restricted_users.py --user NAME --force        # change an existing password
manage_restricted_users.py --new-secret               # also mint a DIS_SESSION_SECRET
```

`--list`, `--remove` and merging all read `DIS_RESTRICTED_USERS` from your
environment unless `--current` is given.

The tool needs only the standard library, so it runs under any `python3`.

## Revoking access

To remove one person, `--remove` them and install the result.

To sign **everybody** out at once — a lost laptop, a password shared too
widely — rotate the session secret. Every existing cookie becomes invalid
immediately:

```bash
python3 utility/bin/manage_restricted_users.py --list --new-secret
# install the printed DIS_SESSION_SECRET, then:
oc -n dis-utilities rollout restart deployment/dis-utilities
```

## Running it locally

```bash
python3 utility/bin/manage_restricted_users.py --user you --new-secret
```

Paste the three `export` lines it prints into the shell you start the server
from, then start the server **in that same shell**.

`DIS_ALLOW_HTTP_SESSION=1` is the one you cannot skip locally. Production is
HTTPS at the router, so the cookie is marked `Secure` by default — and a
`Secure` cookie is silently discarded over plain HTTP. Without it the sign-in
appears to succeed and bounces straight back to the form with nothing in any
log. The sign-in page detects this and says so.

### Testing it in a browser

Browsers cache both the credential and the page, which makes a gate look broken
when it is fine, or fine when it is broken. **Use a private window**, and open a
fresh one for each check:

1. Private window → `/figshare_users` → expect the sign-in page, with
   `?next=/figshare_users` in the address bar
2. Wrong password → stays on the form with a warning
3. Correct password → lands on `/figshare_users`, not the home page
4. The other three now open without prompting again
5. `/restricted_logout`, then `/figshare_users` → prompts again

If a page opens without prompting in a *normal* window, check your server log
first. No log line means the browser served it from cache and never asked.

Note that cookies are scoped by host and **ignore the port**, so two local
instances sharing a secret share sessions — another reason the private window
is the only trustworthy check.

## Known gaps

- `/download/<fname>` is not gated. The filename is
  `name_<random>_<timestamp>.tsv` and can only be learned from a page you had
  to sign in to see, so it is covered in practice, but it is a capability URL
  and the file sits in `/tmp` until the container restarts.
- The navigation menu lists all four pages to everyone. Someone without access
  sees the entry and meets the sign-in page. Hiding them would mean teaching the
  navbar about users, which is the coupling this design avoids.
- There is no password reset, no lockout after repeated failures, and no expiry
  on a session beyond closing the browser. All three are deliberate: this is a
  stopgap for a handful of readers on an internal site, not an auth system.
