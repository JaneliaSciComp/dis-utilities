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

// Recompute row visibility in table tid from its hide set, then refresh the
// visible-row counter (span id counter, if given) and any per-class counters
// the page provides as elements with data-filter-count="<row class>".
function applyRowFilters(tid, counter) {
  const hidden = hiddenClasses(tid);
  const required = requiredTag[tid];
  const classCounts = {};
  let visible = 0;
  $('#' + tid + ' > tbody > tr').each(function () {
    const classes = (this.className || '').split(/\s+/).filter(Boolean);
    const show = !classes.some(cls => hidden.has(cls)) &&
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

// Copy text to the clipboard.
async function copyText(textToCopy) {
  navigator.permissions.query({name: "clipboard-write"});
  try {
    await navigator.clipboard.writeText(textToCopy);
  } catch (err) {
    console.error('Failed to copy text: ', err);
  }
}
