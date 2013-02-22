var Hastur = Hastur || {};

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

Hastur.SeriesRickshawProto = {
    // merge the passed-in options with the default options
    merge_options: function (options) {
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

    color_list: function (data, palette) {
        var palette = new Rickshaw.Color.Palette( { scheme: palette || "munin" } )
        var colors = {};

        // extract all message names from the data across all uuids
        data.sort().forEach(function(name) {
            colors[name] = null;
        });

        // sort it once and get colors for every name so the order is
        // consistent as long as new names don't show up
        d3.keys(colors).sort().forEach(function(name) {
            colors[name] = palette.color();
        });

        return colors;
    },

    /*
     * Initialize a hash in Rickshaw format of the new data.
     */
    to_rickshaw_series: function (data, options) {
        var names = {};
        var colors = this.color_list(d3.keys(data), options.palette);

        // Set series colors
        d3.keys(data).forEach(function (name) {
            data[name].color = colors[name];
        });

        if(options.interpolate) {
            switch(options.interpolate) {
            case "none":
                break;
            case "constant":
                // TODO: copy data before interpolating it
                this.constantFill(data);
                break;
            case "zero":
                // TODO: copy data before interpolating it
                Rickshaw.Series.zeroFill(data);
                break;
            default:
                // Error?
                console.debug("Unknown interpolation method: ", options.interpolate);
            }
        }
        // No interpolation specified?  For now, don't do it.
        // However, this will fail for stacked renderers if the x
        // values don't line up.

        return data;
    },

    constantFill: function(series) {
        var timestamps = {};

        series.map(function(s) {
            for(var i = 0; i < s.data.length; i++) {
                timestamps[s.data[i].x] = true;
            }
        });

        var all_ts = d3.keys(timestamps).sort();

        series.map(function(s) {
            var data = s.data;
            var last_value = data[0] ? data[0].y : 0.0;
            var data_offset = 0;

            all_ts.forEach(function(ts) {
                if(data.length <= data_offset ||
                   data[data_offset].x != ts) {
                    data.splice(data_offset, 0, { x: parseInt(ts), y: last_value });
                } else {
                    last_value = data[data_offset].y;
                }
                data_offset += 1;
            });
        });
    },

    allocate_graph: function(options, series) {
        $(this.options.chart).html("");
        $(this.options.legend).html("");
        $(this.options.y_axis).html("");

        var renderer = this.options.renderer;
        if(renderer == "xkcd") {
            renderer = "line";
        }

        var hseries = series;
        if(hseries.hasOwnProperty("addData")) {
            // This is a Rickshaw series -- use it.
        } else if(typeof(hseries) == "object") {
            // This should be wrapped in a Rickshaw series.
            hseries = new Rickshaw.Series(series);
        } else {
            console.debug("Non-series passed as data to SeriesRickshaw: ", series);
            return null;
        }

        this.graph = new Rickshaw.Graph({
            element: this.options.chart,
            width: this.options.width,
            height: this.options.height,
            renderer: renderer,
            series: hseries
        });

        this.series = hseries;

        // Rickshaw doesn't really have renderer extensions, so
        // we're Flintstones-ing it.
        if(this.options.renderer == "xkcd") {
            this.graph.registerRenderer(new Rickshaw.Graph.Renderer.Xkcd({ graph: this.graph }));
            this.graph.setRenderer("xkcd", this.options);
            this.graph.renderer.wobble = this.options["wobble"] || 0.003;
        }

        if(this.options.renderer_options) {
            this.graph.renderer.configure(this.options.renderer_options);
        }

        var x_axis = new Rickshaw.Graph.Axis.Time({ graph: this.graph });
        var y_axis;

        if (this.options.hasOwnProperty("y_axis")) {
            if(!this.options.y_axis) {
                // Do nothing - it's null or false
            } else if(this.options.y_axis === true) {
                // Draw right on the graph
                y_axis = new Rickshaw.Graph.Axis.Y({
                    orientation: this.options.orientation || 'left',
                    tickFormat: this.options.number_format || Rickshaw.Fixtures.Number.formatKMBT,
                    graph: this.graph
                });
            } else {
                y_axis = new Rickshaw.Graph.Axis.Y({
                    orientation: this.options.orientation || 'left',
                    tickFormat: this.options.number_format || Rickshaw.Fixtures.Number.formatKMBT,
                    graph: this.graph,
                    element: this.options.y_axis
                });
            }
        }

        if (this.options.hasOwnProperty("legend")) {
            this.legend = new Rickshaw.Graph.Legend({
                graph: this.graph,
                element: this.options.legend
            });

            if(this.options.series_toggle) {
                var seriesToggle = new Rickshaw.Graph.Behavior.Series.Toggle( {
	            graph: this.graph,
	            legend: this.legend
                } );
            }

            if(this.options.series_order) {
                var order = new Rickshaw.Graph.Behavior.Series.Order( {
	            graph: this.graph,
	            legend: this.legend
                } );
            }

            if(this.options.series_highlight) {
                var highlighter = new Rickshaw.Graph.Behavior.Series.Highlight( {
	            graph: this.graph,
	            legend: this.legend
                } );
            }
        }

        if(this.options.hover_detail) {
            var hoverDetail = new Rickshaw.Graph.HoverDetail({ graph: this.graph });
        }

        if(this.options.range_slider) {
            var slider = new Rickshaw.Graph.RangeSlider( {
	        graph: this.graph,
	        element: this.options.range_slider
            } );

        }

        return this.graph;
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

        var rickshaw_series = this;

        var refresh_func = function(data) {
            this.has_refreshed = true;

            var new_series = this.to_rickshaw_series(data, this.options);
            if(!this.graph || this.full_refresh) {
                this.allocate_graph(this.options, new_series);
                this.graph.render();
                this.full_refresh = false;
                return;
            }

            this.graph.update();
        };

        this.applied_refresh_func = function() {
            refresh_func.apply(rickshaw_series, [series_source.latest_data()]);
        };

        series_source.on_new_data(this.applied_refresh_func);

        return this.graph;
    },

    immediate_refresh: function(full_refresh) {
        if(full_refresh) {
            this.full_refresh = true;
        }
        if(this.has_refreshed) { this.applied_refresh_func(); }
    },

    set_options: function(options_to_add, options_to_delete) {
        options_to_delete.forEach(function(option) {
            delete this.options[option];
        });
        d3.keys(options).forEach(function(key) {
            this.options[key] = options_to_add[key];
        });

        // I'm sure not literally every option requires an
        // immediate full refresh.  Call me lazy.
        this.immediate_refresh(true);
    },

    // This is a shared list of every SeriesRickshaw
    all_series_rickshaws: []
}

Hastur.SeriesRickshaw = function() {
     Hastur.SeriesRickshawProto.all_series_rickshaws.push(this);

   /*
     * TODO(noah):
     *
     * -> Smoother
     * -> Range slider
     * -> Annotation track
     *
     */

    /*
     * default options for rendering functions
     */
    this.defaults = {
        width: 800,
        height: 400,
        chart: document.querySelector("#chart"),
        interpolate: "zero",   // TODO: remove this after upgrading to latest Rickshaw
        renderer: "line",
        series_toggle: true,
        series_order: true,
        series_highlight: true,
        hover_detail: true
    };
};

Hastur.SeriesRickshaw.prototype = Hastur.SeriesRickshawProto;
