/* Shared DIS helpers, loaded once in base.html.
   These are invoked from server-generated onclick= attributes (see dis_responder.py),
   so they must remain global. */

// POST a field/value (optionally a source) to the custom DOI view.
function nav_post(field, value, source = "") {
  let url = "/doiui/custom";
  let payload = '<input type="text" name="field" value="' + field + '" />' +
                '<input type="text" name="value" value="' + value + '" />';
  if (source) {
    payload = payload +
              '<input type="text" name="jrc_obtained_from" value="' + source + '" />';
  }
  const form = $('<form style="visibility:hidden" action="' + url + '" method="post">' +
                 payload + '</form>');
  $('body').append(form);
  form.submit();
}

// As nav_post, but scopes the custom DOI view to a single year (or "All").
function nav_post_year(field, value, year = "All") {
  let url = "/doiui/custom";
  const payload = '<input type="text" name="field" value="' + field + '" />' +
                  '<input type="text" name="value" value="' + value + '" />';
  if (year != 'All') {
    url = url + "/" + year;
  }
  const form = $('<form style="visibility:hidden" action="' + url + '" method="post">' +
                 payload + '</form>');
  $('body').append(form);
  form.submit();
}

// Shared row-filter state: table id -> Set of CSS classes to hide. A row is
// hidden if it carries any class in the set, so multiple filter buttons
// (e.g. the versioned-DOI toggle and the internal/external cycle) compose
// correctly on the same table.
const rowFilters = {};

function hiddenClasses(tid) {
  if (!rowFilters[tid]) {
    rowFilters[tid] = new Set();
  }
  return rowFilters[tid];
}

// Shared tag-chip filter state: table id -> the single required "tag-<slug>"
// class currently selected (or undefined if no tag chip is active). A row
// must carry this class (in addition to clearing the hiddenClasses check
// above) to stay visible - see filterByTag().
const requiredTag = {};

// Shared free-text filter state: table id -> lowercased query string. A row must
// also contain this substring in its text to stay visible - see filterByText().
const textFilters = {};

// Recompute row visibility in table tid from its hide set, then refresh the
// visible-row counter (span id counter, if given) and any per-class counters
// the page provides as elements with data-filter-count="<row class>".
function applyRowFilters(tid, counter) {
  const hidden = hiddenClasses(tid);
  const required = requiredTag[tid];
  const query = textFilters[tid];
  const classCounts = {};
  let visible = 0;
  $('#' + tid + ' > tbody > tr').each(function () {
    const classes = (this.className || '').split(/\s+/).filter(Boolean);
    const matchesText = !query || (this.textContent || '').toLowerCase().indexOf(query) !== -1;
    const show = matchesText && !classes.some(cls => hidden.has(cls)) &&
                 (!required || classes.includes(required));
    $(this).toggle(show);
    if (show) {
      visible += 1;
      classes.forEach(cls => { classCounts[cls] = (classCounts[cls] || 0) + 1; });
    }
  });
  if (counter) {
    $('#' + counter).text(visible.toLocaleString());
  }
  $('[data-filter-count]').each(function () {
    const cls = $(this).attr('data-filter-count');
    $(this).text((classCounts[cls] || 0).toLocaleString());
  });
}

// Free-text row filter: keep only rows whose text contains the input's value.
// Composes with the class/tag/toggle filters via applyRowFilters().
function filterByText(tid, input, counter) {
  textFilters[tid] = (input.value || '').trim().toLowerCase();
  applyRowFilters(tid, counter);
}

// Filter a class-tagged table: hide/show body rows of class `cls` in table
// `tid`, flip the button label (id btnId) between labelAfterHide/labelAfterShow,
// and recompute footer totals - every tfoot cell carrying
// data-sum-col="<0-based column>" is re-summed over the still-visible rows.
function filterTagTable(tid, btnId, cls, labelAfterHide, labelAfterShow) {
  const rows = $('#' + tid + ' > tbody > tr.' + cls);
  const hide = rows.filter(':visible').length > 0;
  rows.toggle(!hide);
  if (btnId && labelAfterHide && labelAfterShow) {
    $('#' + btnId).text(hide ? labelAfterHide : labelAfterShow);
  }
  $('#' + tid + ' tfoot [data-sum-col]').each(function () {
    const col = parseInt($(this).attr('data-sum-col'), 10);
    let sum = 0;
    $('#' + tid + ' > tbody > tr:visible').each(function () {
      const c = this.cells[col];
      if (c) {
        const n = parseInt((c.textContent || '').replace(/[^\d-]/g, ''), 10);
        if (!isNaN(n)) { sum += n; }
      }
    });
    $(this).text(sum.toLocaleString());
  });
}

// Tag-chip filter: clicking a chip shows only rows carrying its tag-<slug>
// class; clicking the active chip again clears the filter. Composes with
// toggler()/cycle_filter() via applyRowFilters(), so it works alongside the
// version/internal-external/journal-preprint filters on the same table.
function filterByTag(tid, chipEl, counter) {
  const cls = $(chipEl).data('tagclass');
  const wasActive = requiredTag[tid] === cls;
  $(chipEl).closest('p').find('.tag-chip').removeClass('active');
  if (wasActive) {
    delete requiredTag[tid];
  } else {
    requiredTag[tid] = cls;
    $(chipEl).addClass('active');
  }
  applyRowFilters(tid, counter);
}

// Toggle a set of rows (class fid) in table tid, updating the visible-row
// counter and the toggle button's label (button id fid + 'btn', if present).
function toggler(tid, fid, counter) {
  const hidden = hiddenClasses(tid);
  if (hidden.has(fid)) {
    hidden.delete(fid);
    $('#' + fid + 'btn').text('Filter versioned DOIs');
  } else {
    hidden.add(fid);
    $('#' + fid + 'btn').text('Show versioned DOIs');
  }
  applyRowFilters(tid, counter);
}

// Generic class toggle (like toggler, but with caller-supplied labels): hide/show
// rows of class `cls` in table `tid`, flipping button `btn`'s label between
// `whenHidden` (rows now hidden) and `whenShown` (rows now visible). Composes with the
// other filters via the shared hidden-class set + applyRowFilters.
function toggleClass(tid, cls, btn, whenHidden, whenShown, counter) {
  const hidden = hiddenClasses(tid);
  if (hidden.has(cls)) { hidden.delete(cls); $(btn).text(whenShown); }
  else { hidden.add(cls); $(btn).text(whenHidden); }
  applyRowFilters(tid, counter);
}

// Cycling filter: rotates a table's rows through "A & B" -> "A only" -> "B only".
// The button label shows the current view. Composes with toggler() via the
// shared hide set, and keeps the counter span (if given) up to date.
function cycle_filter(btn, tid, ca, cb, la, lb, counter) {
  const state = (parseInt($(btn).attr('data-state') || '0') + 1) % 3;
  $(btn).attr('data-state', state);
  const hidden = hiddenClasses(tid);
  hidden.delete(ca);
  hidden.delete(cb);
  if (state === 1) {
    hidden.add(cb);
    $(btn).text('Showing ' + la + ' only');
  } else if (state === 2) {
    hidden.add(ca);
    $(btn).text('Showing ' + lb + ' only');
  } else {
    $(btn).text('Showing ' + la + ' & ' + lb);
  }
  applyRowFilters(tid, counter);
}

// Live typeahead for the source_metrics tag pulldown: keep only the tag links
// whose text contains the query (case-insensitive), and toggle a "no matching
// tags" placeholder. Bound via oninput on the sticky search box; "All tags" and
// the current selection are unaffected.
function filterTagPulldown(input) {
  const q = (input.value || '').trim().toLowerCase();
  const menu = input.closest('.dropdown-menu');
  if (!menu) { return; }
  let shown = 0;
  menu.querySelectorAll('.tag-pulldown-item').forEach(function (a) {
    const match = (a.textContent || '').toLowerCase().indexOf(q) !== -1;
    a.style.display = match ? '' : 'none';
    if (match) { shown += 1; }
  });
  const empty = menu.querySelector('.tag-pulldown-empty');
  if (empty) { empty.style.display = shown ? 'none' : ''; }
}

// When a tag pulldown opens, clear any stale filter (so the full list is shown)
// and focus the search box so the user can type immediately.
$(document).on('shown.bs.dropdown', function (e) {
  const box = $(e.target).find('.tag-pulldown-search-box');
  if (box.length) {
    box.val('');
    filterTagPulldown(box[0]);
    box.trigger('focus');
  }
});

// Fetch a DOI's BibTeX from our /bibtex endpoint (which fronts
// doi_common.get_bibtex) and copy it to the clipboard. Going through the server
// keeps this independent of Crossref's CORS policy. The icon flips to a check
// mark on success; a DOI with no BibTeX reports rather than copying nothing.
async function copyBibtex(doi, btn) {
  const icon = btn ? btn.querySelector('i') : null;
  const original = icon ? icon.className : null;
  try {
    const resp = await fetch('/bibtex/' + encodeURI(doi));
    if (!resp.ok) { throw new Error('HTTP ' + resp.status); }
    const text = (await resp.text()).trim();
    if (!text) { throw new Error('empty response'); }
    await navigator.clipboard.writeText(text);
    if (icon) {
      icon.className = 'fas fa-check shadow';
      setTimeout(function () { icon.className = original; }, 1200);
    }
  } catch (err) {
    console.error('BibTeX copy failed for ' + doi + ':', err);
    alert('Could not get BibTeX for ' + doi);
  }
}

// Fetch a DOI formatted in a named citation style from our /citation/style
// endpoint (which fronts doi_common.get_citation) and copy it to the clipboard.
// Going through the server keeps this independent of the registrars' CORS
// policies, exactly as copyBibtex does. Fetching happens on click, not on page
// load, so a visit costs the rate-limited formatters nothing.
async function copyCitation(el) {
  const original = el ? el.innerHTML : null;
  // Both values come off the DOM, not from interpolated arguments - see the
  // comment on citation_style_pulldown for why.
  const style = el.dataset.citeStyle;
  const group = el.closest('[data-cite-doi]');
  const doi = group ? group.dataset.citeDoi : '';
  try {
    if (!doi || !style) { throw new Error('missing doi or style'); }
    const resp = await fetch('/citation/style/' + encodeURIComponent(style) +
                             '/' + encodeURI(doi));
    if (!resp.ok) { throw new Error('HTTP ' + resp.status); }
    const text = (await resp.text()).trim();
    if (!text) { throw new Error('empty response'); }
    await navigator.clipboard.writeText(text);
    if (el) {
      el.innerHTML = '<i class="fas fa-check"></i> Copied';
      setTimeout(function () { el.innerHTML = original; }, 1200);
    }
  } catch (err) {
    console.error('Citation copy failed:', err);
    alert('Could not get that citation for this DOI.');
  }
}

// Copy text to the clipboard.
async function copyText(textToCopy) {
  navigator.permissions.query({name: "clipboard-write"});
  try {
    await navigator.clipboard.writeText(textToCopy);
  } catch (err) {
    console.error('Failed to copy text: ', err);
  }
}
