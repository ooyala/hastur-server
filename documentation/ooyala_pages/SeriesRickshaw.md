This is a binding of [Hastur/SeriesSource](SeriesSource) to the [Rickshaw](http://code.shutterstock.com/rickshaw/) JavaScript graphing library, based on [D3](http://d3js.org).

~~~
var hastur_palette = ["#FA5833", "#b9e672", "#909090", "#2FABE9", "#e7e572", "#e42b75", "#f4a70c"];

(new Hastur.SeriesRickshaw()).start_graphing({
  chart: document.getElementById("handled_rtt"),
  legend: document.getElementById("handled_legend"),
  y_axis: document.getElementById("handled_y"),
  palette: hastur_palette,
  width: false,
  height: false
}, {
  name: prefix + "requests",
  rollup: "five_minutes",
  ago: "one_day",
  accessor: "sum",
  fun: "hostname()",
  autorefresh: 15000
});
~~~

When creating a SeriesRickshaw, you pass one JS object full of graphing library parameters and one of [Hastur/SeriesSource](SeriesSource) parameters, as shown above.

Some graphing library parameters like "chart" and "legend" specify where in the HTML DOM to put your graph, while others like palette specify how the graph should appear.

SeriesSource parameters are documented at [Hastur/SeriesSource](SeriesSource) and will not be repeated here.

## SeriesRickshaw Options

* width: width in pixels, or false for "use element's width".  Default: 800
* height: height in pixels, or false for "use element's height".  Default: 400
* chart: DOM element for graph.  Default: document.querySelector("#chart")
* legend: DOM element for legend.  Default: none
* y_axis: DOM element for Y axis, or "true" for draw-on-graph.  Default: none
* renderer: line, stack or other Rickshaw renderer type.
* renderer_options: JS object with option to configure for Rickshaw renderer.
* palette: name of Rickshaw palette or array of JS color strings like "#ff00aa".
* hover_detail: mouseover a point to see information on that point.  Default: true
* series_toggle: in legend, allow toggling series off.  Default: true
* series_order: in legend, allow drag to reorder series.  Default: true
* series_highlight: in legend, mouseover name to highlight series.  Default: true
* interpolate: constant or zero to fill in elements "missing" in series.
