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

// Toggle a set of rows (class fid) in table tid, updating the visible-row counter
// and the toggle button's label.
function toggler(tid, fid, counter) {
  $('.' + fid).toggle();
  const total = $('#' + tid + ' >tbody >tr:visible').length;
  $('#' + counter).text(total);
  if ($('#' + tid + ' >tbody >tr').is(":hidden")) {
    $('#' + fid + 'btn').text('Show versioned DOIs');
  } else {
    $('#' + fid + 'btn').text('Filter versioned DOIs');
  }
}

// Cycling filter: rotates a table's rows through "A & B" -> "A only" -> "B only".
// The button label shows the current view. Counts are left untouched.
function cycle_filter(btn, tid, ca, cb, la, lb) {
  const state = (parseInt($(btn).attr('data-state') || '0') + 1) % 3;
  $(btn).attr('data-state', state);
  if (state === 0) {
    $('#' + tid + ' .' + ca).show();
    $('#' + tid + ' .' + cb).show();
    $(btn).text('Showing ' + la + ' & ' + lb);
  } else if (state === 1) {
    $('#' + tid + ' .' + ca).show();
    $('#' + tid + ' .' + cb).hide();
    $(btn).text('Showing ' + la + ' only');
  } else {
    $('#' + tid + ' .' + ca).hide();
    $('#' + tid + ' .' + cb).show();
    $(btn).text('Showing ' + lb + ' only');
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
