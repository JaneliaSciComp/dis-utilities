// Make tablesorter's numeric parsers comma-aware. The vendored jquery.tablesorter
// formatFloat() runs parseFloat() without stripping thousands separators, so a cell
// showing "1,234" sorts as 1. Override it once (this file loads after
// jquery.tablesorter.js) so every numeric/currency column sorts by its true value.
// Per-cell data-sort (via cell(sort=) in dis_responder.py) remains the tool for
// cases where the sort key differs from the displayed text (dates, day counts, etc.).
if (window.jQuery && jQuery.tablesorter) {
  jQuery.tablesorter.formatFloat = function (s) {
    var i = parseFloat(String(s).replace(/,/g, ''));
    return isNaN(i) ? 0 : i;
  };
}

function tableInitialize () {
  $(document).ready(function() {
    $("table").each(function() {
      if ($(this).is('.tablesorter')) {
        var opts = {
          textExtraction: function(node) {
            var sort = $(node).attr('data-sort');
            return sort !== undefined ? sort : $(node).text().trim();
          }
        };
        var sl = $(this).attr('data-sortlist');
        if (sl) { opts.sortList = JSON.parse(sl); }
        $(this).tablesorter(opts);
      }
    });
    // Seed any default row filters declared on a table. data-initial-hide is a
    // space-separated list of row classes to hide on load (e.g. a cycle_filter
    // button rendered pre-set to one of its states); data-counter names the
    // visible-row counter span to keep in sync. Composes with the dis.js filters
    // via the shared hide set. Requires dis.js (hiddenClasses/applyRowFilters).
    if (typeof hiddenClasses === 'function' && typeof applyRowFilters === 'function') {
      $('table[data-initial-hide]').each(function() {
        var tid = this.id;
        if (!tid) {
          return;
        }
        var hidden = hiddenClasses(tid);
        ($(this).attr('data-initial-hide') || '').split(/\s+/).filter(Boolean)
          .forEach(function(cls) { hidden.add(cls); });
        applyRowFilters(tid, $(this).attr('data-counter') || null);
      });
    }
  });
}

