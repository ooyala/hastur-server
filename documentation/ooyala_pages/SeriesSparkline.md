This is a binding of [Hastur/SeriesSource]() to the [jQuery Sparkline](http://omnipotent.net/jquery.sparkline/#s-about) JavaScript graphing library.

~~~
var hastur_palette = ["#FA5833", "#b9e672", "#909090", "#2FABE9", "#e7e572", "#e42b75", "#f4a70c"];

(new Hastur.SeriesSparkline()).start_graphing({
  width: 120, //Width of the chart - Defaults to 'auto'
  height: 30, //Height of the chart - Defaults to 'auto' (line height of the containing tag)
  lineColor: hastur_palette, //Used by line and discrete charts to specify the colour of the line drawn as a CSS values string
  fillColor: false, //Specify the colour used to fill the area under the graph as a CSS value. Set to false to disable fill
  spotColor: '#467e8c', //The CSS colour of the final value marker. Set to false or an empty string to hide it
  maxSpotColor: '#b9e672', //The CSS colour of the marker displayed for the maximum value. Set to false or an empty string to hide it
  minSpotColor: '#FA5833', //The CSS colour of the marker displayed for the mimum value. Set to false or an empty string to hide it
  spotRadius: 2, //Radius of all spot markers, In pixels (default: 1.5) - Integer
  lineWidth: 1 //In pixels (default: 1) - Integer
}, {
  chart: "#sparklineGraph",
  name: prefix + "requests",
  rollup: "five_minutes",
  ago: "one_day",
  accessor: "sum",
  fun: "hostname()",
  autorefresh: 15000
});
~~~

When creating a SeriesSparkline, you pass one JS object full of graphing library parameters and one of [Hastur/SeriesSource](SeriesSource) parameters, as shown above.

Some graphing library parameters like "chart" and "legend" specify where in the HTML DOM to put your graph, while others like palette specify how the graph should appear.

SeriesSource parameters are documented at [Hastur/SeriesSource](SeriesSource) and will not be repeated here.

## SeriesSparkline Options

Other than "chart", which should be a DOM element or a DOM selector, just used the [regular Sparkline options](http://omnipotent.net/jquery.sparkline/#s-docs).
