var Hastur = Hastur || {};

/* Configuration TODOs:
  - Configure display_name?  Or just subclass?
  - Configure how timestamps are calculated
*/

/* Hastur.SeriesSource depends on d3, and is designed to be easy to use with Rickshaw. */

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

Hastur.SeriesSourceProto = {
    on_new_data: function(callback) {
        this.data_callbacks.push(callback);

        if(this.series.length > 0) {
            // There's already data.
            callback();
        }
    },

    notify_new_data: function() {
        if(!this.series || this.series.length < 1)
            return;

        this.data_callbacks.forEach(function(callback) {
            callback();
        });
    },

    latest_data: function() {
        return this.series;
    },

    // merge the passed-in options with the default options
    merge_options: function (options) {
        var defaults = this.defaults;

        // copy first
        out = {};
        d3.keys(options).forEach(function (key) {
            out[key] = options[key];
        });

        // apply global and URL defaults that aren't already present
        var defaults = this.defaults;
        d3.keys(defaults).forEach(function (key) {
            if (!out.hasOwnProperty(key))
                out[key] = Hastur.urlParams[key] || defaults[key];
        });

        return out;
    },

    ago_in_seconds: function(ago) {
        var translation = {
            one_second: 1,
            second: 1,
            five_seconds: 5,
            one_minute: 60,
            minute: 60,
            five_minutes: 5 * 60,
            one_hour: 60 * 60,
            hour: 60 * 60,
            one_day: 24 * 60 * 60,
            day: 24 * 60 * 60,
            two_days: 2 * 24 * 60 * 60,
            one_week: 7 * 24 * 60 * 60,
            two_weeks: 2 * 7 * 24 * 60 * 60
        };

        if(translation[ago])
            return translation[ago];

        if(parseInt(ago) > 0)
            return parseInt(ago);

        console.debug("Not a recognized interval:", ago, "!");
        return 0;
    },

    insert_into_data_map: function(series) {
        if(!this.options.incremental) return;

        var data_map = this.data_map;

        series.forEach(function(subseries) {
            data_map[subseries.name] = data_map[subseries.name] || {};
            var sub_map = data_map[subseries.name];

            subseries.data.forEach(function(data_item) {
                sub_map[data_item.x] = data_item;
            });
        });
    },

    display_name: function(node, name) {
        if (node == "") return name;
        if (name == "") return node;

        return node + ":" + name
    },

    /*
     * Add entries to a series data structure.
     * Input is Hastur-style: UUID -> stat_name -> { ts, value }
     * Internal structure is Name -> [ { ts, value } ]
     */
    insert_into_series: function (data, options) {
        var source_object = this;
        var manual_extraction = false;

        d3.keys(data).forEach(function (node) {
            d3.keys(data[node]).sort().forEach(function (name) {
                var sname = source_object.display_name(node, name);

                if(!source_object.data_map[sname])
                    source_object.data_map[sname] = {};

                var internal_series;
                for(var i = 0; i < source_object.series.length; i++) {
                    if(source_object.series[i].name === sname) {
                        internal_series = source_object.series[i];
                        break;
                    }
                }
                if(!internal_series) {
                    internal_series = {
                        name: sname,
                        data: []
                    }
                    source_object.series.push(internal_series);
                }
                var items = internal_series.data;

                if(!source_object.options.incremental) {
                    items.splice(0, items.length);
                }

	        var last_ts = -1;
                d3.keys(data[node][name]).sort().forEach(function (tss) {
                    tss = parseInt(tss);

                    if(!source_object.max_tss || tss > source_object.max_tss)
                        source_object.max_tss = tss;

                    var ts = parseInt(tss / 1000000);
                    if(!source_object.max_ts || ts > source_object.max_ts)
                        source_object.max_ts = ts;
                    var val = data[node][name][tss];
                    if(ts == last_ts) return;  // No duplicates from same series
	            last_ts = ts;

                    // Hastur raw messages
                    var data_item = null;

                    // Rollups have "average" property
                    if (typeof(val) === "object" && val.hasOwnProperty("average")) {
                        // If an accessor is given, use it
                        if(options["accessor"]) {
                            data_item = { x: ts, y: val[options["accessor"]]};
                        } else {
                            // A library can't directly use this, but a TransformSource or
                            // hand-rolled renderer can get good results from it.
                            data_item = { x: ts, y: val };
                            manual_extraction = true;
                        }
                    // Raw non-rolled messages have "value" property
                    } else if (typeof(val) === "object" && val.hasOwnProperty("value"))
                        data_item = { x: ts, y: val.value };

                    // Or else just a number
                    else if (typeof(val) === "number")
                        data_item = { x: ts, y: val };
                    else
                        console.log("Unusable entry in data: ", node, name, tss, typeof(val), val, ts);

                    if(data_item) {
                        if(source_object.data_map[sname][ts]) {
                            // Overwrite value
                            source_object.data_map[sname][ts].y = data_item.y;
                        } else {
                            // TODO: figure out ordering problems here for incremental!
                            items.push(data_item);
                            if(source_object.options.incremental)
                                source_object.data_map[sname][ts] = data_item;
                        }
                    }
                });
            });
        });

        if(manual_extraction && !this.warned_manual_extraction) {
            console.debug("You specified at least one series that's just passed along as a rollup." +
                          "  Make sure to extract it later or you'll get rendering errors " +
                          "from your library.");
            this.warned_manual_extraction = true;
        }
    },

    trim_series_before: function(timestamp) {
        this.series.forEach(function(subseries) {
            while(subseries.data.length > 0
                  && subseries.data[0].x < timestamp) {
                subseries.data.splice(0, 1);
            }
        });
    },

    url_for_options: function(options) {
        var params = [];
        var names = [].concat(options.name);
        var format = options["format"];
        if (options.hasOwnProperty("rollup")) {
            format = "rollup";
            params.push("rollup_period=" + options.rollup);
        }

        var host = "";
        if (options.hasOwnProperty("host")) {
            host = options.host;
            if(options.host.slice(0, 4) != "http")
                host = "http://" + host;
        }

        ["ago", "fun", "start", "end", "uuid", "label", "type"].forEach(function (key) {
            if (options.hasOwnProperty(key) && options[key] != false)
                params.push(key + "=" + encodeURIComponent(options[key]));
        });

        if(options["v2"]) {
            return host + "/v2/query?name=" + names.join() + "&kind="+ format +"&" + params.join("&");
        } else {
            return host + "/api/name/" + names.join() + "/"+ format +"?" + params.join("&");
        }
    },

    query: function(options_in) {
        this.options = this.merge_options(options_in);
        var options = this.options;

        if(!options.name) {
            console.debug("No 'name' option specified to Hastur.SeriesSource!");
        }

        var url = this.url_for_options(options);
        var source_object = this;

        console.log("SeriesSource URL: ", url);

        var refresh_func = function() {
            d3.json(url, function (data) {
                source_object.insert_into_series(data, source_object.options);
                source_object.notify_new_data(source_object.series);
                if(source_object.options.incremental) {
                    // Query from the most recently received sample, minus a five-second grace time.
                    // This could go wrong with rollups and variable latency - test!
                    delete source_object.options.ago
                    source_object.options.start = source_object.max_tss - 5000000;
                    url = source_object.url_for_options(source_object.options);

                    var orig_ago = source_object.original_options.ago;
                    if(orig_ago) {
                        var earliest_ts = source_object.max_ts - source_object.ago_in_seconds(orig_ago);
                        source_object.trim_series_before(earliest_ts);
                    }
                }
            });
        };

        this.applied_refresh_func = function() {
            refresh_func.apply(source_object, []);
        };

        if(options["autorefresh"]) {
	    this.refresh_funcs.push(window.setInterval(this.applied_refresh_func, parseInt(options["autorefresh"])));
        }
	this.applied_refresh_func();
    },

    reset: function(requery) {
        // Delete stuff that incremental update bashes.
        delete this.options.ago;
        delete this.options.start;

        if(this.original_options.ago)
            this.options.ago = this.original_options.ago;
        if(this.original_options.start)
            this.options.start = this.original_options.start;

        this.data_map = {};
        this.series = [];

        if(requery) this.applied_refresh_func();
    },

    set_options: function(options_to_add, options_to_delete) {
        options_to_delete.forEach(function(option) {
            delete this.options[key];
            delete this.original_options[key];
        });
        d3.keys(options).forEach(function(key) {
            this.options[key] = options_to_add[key];
            this.original_options[key] = options_to_add[key];
        });

        // I'm sure not literally every option requires an
        // immediate full refresh.  Call me lazy.
        this.reset(true);
    },

    // This is a shared list of every SeriesSource
    all_series_sources: []
};

Hastur.SeriesSource = function(options) {
    Hastur.SeriesSourceProto.all_series_sources.push(this);

    this.defaults = {
        // start:
        // end:
        // uuid:
        // label:
        // autorefresh:
        v2: false,
        host: "http://hastur.ooyala.com",
        fun: "hostname()",
        format: "value",
        incremental: false,
        ago: "five_minutes"
    };
    var original_options = {};
    this.original_options = original_options;
    d3.keys(options).forEach(function(key) {
        original_options[key] = options[key];
    });
    this.refresh_funcs = [];
    this.data_callbacks = [];
    this.data_map = {};

    this.series = [];

    this.query(options);
};

Hastur.SeriesSource.prototype = Hastur.SeriesSourceProto;

Hastur.TransformSourceProto = {
    on_new_data: function(callback) {
        this.data_callbacks.push(callback);
    },

    notify_new_data: function() {
        this.data_callbacks.forEach(function(callback) {
            callback();
        });
    },

    latest_data: function() {
        return this.series;
    },

    transform_series: function(series) {
        var data_out = [];
        var value_transform = this.value_transform;

        if(this.series_transform) {
            return this.series_transform(series);
        }

        if(this.subseries_transform) {
            return series.map(this.subseries_transform);
        }

        series.forEach(function(subseries) {
            var subseries_out = { name: subseries.name, data: [] };

            subseries.data.forEach(function(item) {
                subseries_out.data.push({ x: item.x, y: value_transform(item.y) });
            });

            data_out.push(subseries_out);
        });

        return data_out;
    }
};

Hastur.TransformSource = function(options) {
    this.data_callbacks = [];
    this.series = [];
    this.source = options.source;
    this.value_transform = options.value_transform;
    this.subseries_transform = options.subseries_transform;
    this.series_transform = options.series_transform;

    if(!options.source) {
        console.debug("No source option given to TransformSource!");
        return;
    }

    if(!options.value_transform && !options.subseries_transform && !options.series_transform) {
        console.debug("No value_transform, subseries_transform or series_transform option(s) " +
                      "given to TransformSource!  Failing.");
        return;
    }

    if((options.value_transform && (options.subseries_transform || options.series_transform))
       || (options.subseries_transform && (options.value_transform || options.series_transform))
       || (options.series_transform && (options.subseries_transform || options.value_transform))) {
        console.debug("You gave more than one of value_transform, subseries_transform or series_transform " +
                      "option(s) to TransformSource!  Failing.");
        return;
    }

    var this_source = this.source;
    var this_transform_source = this;

    this.source.on_new_data(function() {
        var data = this_source.latest_data();
        var data_out = this_transform_source.transform_series.apply(this_transform_source, [data]);

        this_transform_source.series = data_out;
        this_transform_source.notify_new_data();
    });
};

Hastur.TransformSource.prototype = Hastur.TransformSourceProto;
