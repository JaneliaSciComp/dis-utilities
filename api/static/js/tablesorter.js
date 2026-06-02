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

