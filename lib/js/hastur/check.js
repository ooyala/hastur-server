var hastur = hastur || {};

hastur.check = {
  /*
   * Check a given stat name as a heartbeat and return true/false if all included
   * nodes have reported within the given timeout.
   *
   * function (name, timeout, options)
   *   name - hastur stat name, e.g. hastur.agent.heartbeat
   *   timeout - timeout in seconds
   *   options:
   *     ago, fun, start, end, uuid, label, count
   *   cb: callback to call when the check is complete, passes the result of true/false
   * example: hastur.check.heartbeat("hastur.agent.heartbeat", 60, {}, function (result) { })
   */
  heartbeat: function (name, timeout, options, cb) {
    var params = [];

    if (!options.hasOwnProperty("ago"))
      options["ago"] = "five_minutes"

    ["ago", "fun", "start", "end", "uuid", "label", "count"].forEach(function (key) {
      if (options.hasOwnProperty(key) && options[key] != false)
        params.push(key + "=" + encodeURIComponent(options[key]));
    });

    var url = "/api/name/" + name + "/value?" + params.join("&")

    d3.json(url, function (data) {
      var now = Date.now() / 1000; /* epoch seconds */
      var result = true;

      d3.keys(data).forEach(function (node) {
        d3.keys(data[node]).forEach(function (name) {
          var ok = false;
          d3.keys(data[node][name]).forEach(function (tss) {
            var ts = parseInt(tss);
            if (now - ts / 1000000 < timeout)
              ok = true;
          });
          if (ok == false)
            result = false;
        });
      });

      cb.call(this, result);
    });
  }
};
