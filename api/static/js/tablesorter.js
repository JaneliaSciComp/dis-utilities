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
        $(this).tablesorter({
          textExtraction: function(node) {
            var sort = $(node).attr('data-sort');
            return sort !== undefined ? sort : $(node).text().trim();
          }
        });
      }
    });
  });
}

