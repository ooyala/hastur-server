var Hastur = Hastur || {}

// http://stackoverflow.com/questions/901115/how-can-i-get-query-string-values
if(!Hastur.urlParams) {
    Hastur.urlParams = {};
    (function () {
        var match,
        pl     = /\+/g,  // Regex for replacing addition symbol with a space
        search = /([^&=]+)=?([^&]*)/g,
        decode = function (s) { return decodeURIComponent(s.replace(pl, " ")); },
        query  = window.location.search.substring(1);

        while (match = search.exec(query))
            Hastur.urlParams[decode(match[1])] = decode(match[2]);
    })();
}

Hastur.SeriesSparklineProto = {
    // merge the passed-in options with the default options
    merge_options: function (options) {
        // copy first
        out = {};
        d3.keys(options).forEach(function (key) {
            out[key] = options[key];
        });

        // apply global defaults that aren't already present
        var defaults = this.defaults;
        d3.keys(defaults).forEach(function (key) {
            if (!out.hasOwnProperty(key))
                out[key] = defaults[key];
        });

        return out;
    },

    start_graphing: function(options_in, series_source) {
        this.options = this.merge_options(options_in);

        if("on_new_data" in series_source) {
            // Keep as series source
        } else if (typeof(series_source) == "object") {
            series_source = new Hastur.SeriesSource(series_source);
        } else {
            console.debug("Ignoring -- not a Hastur source:", series_source);
            return;
        }

        var refresh_func = function(data) {
            var options = this.options;

            // Clear old graph
            $(options.chart).html("");

            // Default options
            var sparkline_options = {
                type: "line"
            };

            // Array-valued options change from subseries to subseries
            var array_options = {};
            d3.keys(options).forEach(function(key) {
                var value = options[key];
                if(typeof(value) === "object") {
                    array_options[key] = value;
                } else {
                    sparkline_options[key] = value;
                }
            });

            var offset = 0;
            data.forEach(function(subseries) {
                var sub_data = []

                subseries.data.forEach(function(item) {
                    sub_data.push([item.x, item.y]);
                });

                // Set array-valued options for this subseries
                d3.keys(array_options).forEach(function(key) {
                    sparkline_options[key] = array_options[key][offset];
                });
                $(options.chart).sparkline(sub_data, sparkline_options);
                offset++;

                sparkline_options.composite = true;
            });
        };

        var sparkline_series = this;
        var applied_refresh_func = function() {
            refresh_func.apply(sparkline_series, [series_source.latest_data()]);
        };

        series_source.on_new_data(applied_refresh_func);

        return this;
    }
};

Hastur.SeriesSparkline = function() {

};

Hastur.SeriesSparkline.prototype = Hastur.SeriesSparklineProto;
