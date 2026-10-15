<!--
  Open with the governing fact, not a summary of the diff. The best PRs in this
  repo start with the thing that made the change necessary or constrained how it
  was done - "project_map is the one collection no program writes", "528 fetch
  errors, almost none of them errors" - and the diff follows from it.

  Three habits worth keeping:

    - Cite evidence rather than asserting. Concrete DOIs, record IDs, counts,
      before/after numbers. "Verified against the endpoint rather than assumed"
      is a sentence worth being able to write.
    - Say what you deliberately did NOT do, and why. Rejected alternatives and
      narrower-than-expected scope are the first things lost to time, and the
      first things the next reader wants.
    - Reach for a table when the rule differs per field, per case or per route.

  No checklist here on purpose: tests.yml runs the full suite on 3.10 and 3.12
  for every PR, so nothing below asks you to confirm what CI already proves.

  Delete this comment or leave it - it does not render either way.
-->


***

<!--
  The four facts CI cannot check. Answer every line; "none" is a real answer and
  a useful one. Keep them as a footer so the body above stays prose.
-->

**Deploy:** <!-- Which artifact this ships in, and its version. There are two
  independent paths, and they do not work the same way.

  api/ -> OpenShift, "ocbuild dis-utilities <version>". The tag IS
  api/dis_responder.py __version__ - one artifact, one version, covering
  everything in api/: dis_html.py, templates/, static/, config.cfg. No other
  module there carries a version, deliberately. Bump it or ocbuild warns that
  the tag already exists. It has drifted before: #551 was a catch-up for 40
  commits.

  sync/ and sync/acknowledgements/ -> one Docker image, "ecrdeploy dis-sync
  <version>", which the nightly Jenkins jobs pull. The image tag is its own
  semver line (dis-sync 57.x), NOT any program's __version__: 40 programs there
  carry one, and each is informational - it appears in that program's report
  and email. Both still want bumping, for different reasons.

  The sync trap: the Dockerfile copies all of bin/* and acknowledgements/*.py,
  so one image carries every program. Deploying a fix to one also ships
  whatever else was committed to those directories since the last push. Say so
  if this PR is riding along with someone else's change.

  "no deploy" if this touches neither tree - this template itself did not. -->

**Writes to production:** <!-- Which collections and fields this writes, and
  whether a hand migration has already been run. Say "none" for a read-only
  change. -->

**New or changed routes:** <!-- Path, and @restricted / @restricted('term') /
  deliberately public - with the reason for public. Several pages here are
  genuinely public, so this is not boilerplate: a control rendered without a
  gate dead-ends every ordinary reader on the denied page. -->

**Sync interaction:** <!-- Any sync program that writes the same fields, and how
  the two avoid clobbering each other. Name-variant lists need $addToSet rather
  than a rebuilt array because three programs write them; alumni has no
  clearing path in any sync at all. "none" if nothing else touches this. -->
