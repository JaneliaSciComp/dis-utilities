# Restricted pages

Four pages name individual people alongside the deposits they are credited on:

| | |
|---|---|
| `/figshare_users` | `/protocolsio_users` |
| `/zenodo_users` | `/dryad_users` |

They require a sign-in. This is not secrecy — the DOIs are public and the roster
is visible elsewhere on the site — it is keeping the convenient aggregation to
the people who need it.

## How it works

Sign-in is with an **ORCID iD** and a password assigned by DIS. A `@restricted`
decorator on the four views sends anyone without a session to `/login`, and on
to the page they asked for once they sign in. One sign-in covers all four.
`/logout` ends it.

**This is temporary.** The site will sign in through **orcid.org** directly, at
which point the password half disappears. The ORCID iD is the part that lasts:
the session holds a real iD, which is exactly what the orcid.org callback will
set, so `current_user()` and everything built on it survives the change
untouched.

The check lives in the application, not at the edge. Production is OpenShift,
and the route runs browser → HAProxy → Service → gunicorn with no proxy of ours
in between. The `nginx.conf` in the repository root is left over from an older
docker-compose deployment and is read by nothing.

## Who may sign in

Two conditions, both enforced at sign-in **and on every subsequent request**:

1. The ORCID iD is in the `orcid` collection.
2. The person passes the access rule:

```
(alumni is not True) or (workerType == 'Contingent Worker')
```

`alumni` means **departed** — it is set with an `alumni_date` when someone drops
out of HHMI People, and cleared when they return. So the rule admits departed
contingent workers; a former employee who came back already has the flag
cleared and counts as current staff.

Because the rule runs on every request, somebody who leaves loses access
without anyone doing anything. Equally, that flag is now a permission rather
than a description: anything that sets or clears it — including the nightly
People sync — is an access change.

## Adding someone

```bash
python3 utility/bin/manage_restricted_users.py --orcid 0000-0002-1825-0097 --write
```

It prompts for a password twice, hashes it, and writes to the `user_auth`
collection. **Effective immediately — no Secret edit, no deployment restart.**

It refuses to issue a password for an iD that is not in the `orcid` collection
or does not pass the access rule, so access cannot be granted to somebody the
roster does not know.

Without `--write` it reports what it would do and changes nothing.

### Other operations

```bash
manage_restricted_users.py --list                       # who can sign in, and when they last did
manage_restricted_users.py --audit                      # credentials whose person no longer qualifies
manage_restricted_users.py --orcid <iD> --force --write # change an existing password
manage_restricted_users.py --remove <iD> --write        # take access away
```

`--manifold` selects the database (`prod` by default).

The tool needs only the standard library plus `jrc_common`, so it runs under any
`python3`.

## Where the credentials live

A `user_auth` collection, keyed by ORCID iD:

```javascript
{ orcid:      "0000-0003-3118-1636",
  password:   "scrypt:32768:8:1$...",    // hash only; no password is stored
  created:    ISODate(...),
  updated:    ISODate(...),              // last password change
  last_login: ISODate(...) }             // written by the application
```

**Deliberately not a field on the `orcid` document.** `/orcid/<oid>`,
`/orcid/active`, `/orcid/works/<oid>` and `/orcidapi/<oid>` all project with
`{'_id': 0}` — which excludes the id and nothing else — so any field added to an
orcid document is published as JSON by endpoints that are open today.

When orcid.org sign-in lands, drop this collection.

## The one setting

`DIS_SESSION_SECRET`, from the `dis-utilities-secrets` Secret, signs the session
cookie.

**It must be identical in every replica.** The deployment runs two, and the
session is a signed cookie with no server-side store, so a per-pod value would
sign people out as the Service moved them between pods. It comes from `envFrom`,
so all pods get the same value — the failure mode only appears if someone
generates it per process.

Rotating it signs **everybody** out immediately. That is the emergency revoke.

If it is unset, the four pages return **503** and say so. Missing configuration
denies access rather than granting it: a page that had quietly stopped being
restricted would look exactly like one that was working.

## Running it locally

```bash
export DIS_SESSION_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")
export DIS_ALLOW_HTTP_SESSION=1
```

`DIS_ALLOW_HTTP_SESSION=1` is the one you cannot skip. Production is HTTPS at
the router, so the cookie is marked `Secure` by default — and a `Secure` cookie
is silently discarded over plain HTTP. Without it the sign-in appears to
succeed and bounces straight back to the form with nothing in any log. The
sign-in page detects this and says so.

### Testing in a browser

Browsers cache both the credential and the page, which makes a gate look broken
when it is fine, or fine when it is broken. **Use a private window**, and open a
fresh one for each check:

1. Private window → `/figshare_users` → expect `/login?next=/figshare_users`
2. Wrong password → stays on the form with a warning
3. Correct → lands on `/figshare_users`, not the home page
4. The other three now open without prompting again
5. `/logout`, then `/figshare_users` → prompts again

If a page opens without prompting in a *normal* window, check the server log
first. No log line means the browser served it from cache and never asked.

Note that cookies are scoped by host and **ignore the port**, so two local
instances sharing a secret share sessions — another reason the private window is
the only trustworthy check.

## The audit trail

Every attempt is logged:

```
Sign-in: 0000-0003-3118-1636 (Nelson Spruston) from 10.x.x.x
Sign-in FAILED for '0000-0003-3118-1636' from 10.x.x.x
Sign-in REFUSED by the access rule: 0000-0002-5795-0635 from 10.x.x.x
```

The third is worth distinguishing: the password was **correct**, but the roster
no longer permits that person. The sign-in page tells them so rather than
claiming the password is wrong, which would have them retyping a password that
is in fact right.

## Known gaps

- `/download/<fname>` is not gated. The filename is
  `name_<random>_<timestamp>.tsv` and can only be learned from a page you had to
  sign in to see, so it is covered in practice, but it is a capability URL and
  the file sits in `/tmp` until the container restarts.
- The navigation menu lists all four pages to everyone. Someone without access
  sees the entry and meets the sign-in page.
- There is no password reset, no lockout after repeated failures, and no session
  expiry beyond closing the browser. All three are deliberate: this is a stopgap
  for a handful of readers on an internal site, pending orcid.org.
- Removing a credential does not end a session that is already open. Rotate
  `DIS_SESSION_SECRET` if somebody must be out immediately.
