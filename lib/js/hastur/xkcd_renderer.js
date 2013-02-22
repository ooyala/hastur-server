Rickshaw.namespace('Rickshaw.Graph.Renderer.Xkcd');

// This file is a weird mashup of Rickshaw internal renderer code
// and the XKCD rendering stuff:
// * http://dan.iel.fm/xkcd/
// * http://bl.ocks.org/3914862

// Return an xinterp function for a given graph
function xkcd_xinterp_on_graph(graph) {
    var domain = graph.renderer.domain();
    var wobble = graph.renderer.wobble || 0.005;
    return function(points) {
	return xkcd_xinterp(graph.x, graph.y, domain.x, domain.y, wobble, points);
    }
}

function xkcd_xinterp(xscale, yscale, xlim, ylim, magnitude, points) {
    // Scale the data.
    var f = [xscale(xlim[1]) - xscale(xlim[0]),
             yscale(ylim[1]) - yscale(ylim[0])],
    z = [xscale(xlim[0]),
         yscale(ylim[0])],
    scaled = points.map(function (p) {
        return [(p[0] - z[0]) / f[0], (p[1] - z[1]) / f[1]];
    });

    // Compute the distance along the path using a map-reduce.
    var dists = scaled.map(function (d, i) {
        if (i == 0) return 0.0;
        var dx = d[0] - scaled[i - 1][0],
            dy = d[1] - scaled[i - 1][1];
        return Math.sqrt(dx * dx + dy * dy);
    });
    var dist = dists.reduce(function (curr, d) { return d + curr; }, 0.0);

    // Choose the number of interpolation points based on this distance.
    var N = Math.round(200 * dist);

    // Re-sample the line.
    var resampled = [];
    dists.map(function (d, i) {
        if (i == 0) return;
        var n = Math.max(3, Math.round(d / dist * N)),
            spline = d3.interpolate(scaled[i - 1][1], scaled[i][1]),
            delta = (scaled[i][0] - scaled[i - 1][0]) / (n - 1);
        for (var j = 0, x = scaled[i - 1][0]; j < n; ++j, x += delta)
            resampled.push([x, spline(j / (n - 1))]);
    });

    // Compute the gradients.
    var gradients = resampled.map(function (a, i, d) {
        if (i == 0) return [d[1][0] - d[0][0], d[1][1] - d[0][1]];
        if (i == resampled.length - 1)
            return [d[i][0] - d[i - 1][0], d[i][1] - d[i - 1][1]];
        return [0.5 * (d[i + 1][0] - d[i - 1][0]),
                0.5 * (d[i + 1][1] - d[i - 1][1])];
    });

    // Normalize the gradient vectors to be unit vectors.
    gradients = gradients.map(function (d) {
        var len = Math.sqrt(d[0] * d[0] + d[1] * d[1]);
        return [d[0] / len, d[1] / len];
    });

    // Generate some perturbations.
    var perturbations = xkcd_smooth(resampled.map(d3.random.normal()), 3);

    // Add in the perturbations and re-scale the re-sampled curve.
    var result = resampled.map(function (d, i) {
        var p = perturbations[i],
            g = gradients[i];
        return [(d[0] + magnitude * g[1] * p) * f[0] + z[0],
                (d[1] - magnitude * g[0] * p) * f[1] + z[1]];
    });


    return result.join("L");
}

// Smooth some data with a given window size.
function xkcd_smooth(d, w) {
    var result = [];
    for (var i = 0, l = d.length; i < l; ++i) {
        var mn = Math.max(0, i - 5 * w),
            mx = Math.min(d.length - 1, i + 5 * w),
            s = 0.0;
        result[i] = 0.0;
        for (var j = mn; j < mx; ++j) {
            var wd = Math.exp(-0.5 * (i - j) * (i - j) / w / w);
            result[i] += wd * d[j];
            s += wd;
        }
        result[i] /= s;
    }
    return result;
}

Rickshaw.Graph.Renderer.Xkcd = Rickshaw.Class.create( Rickshaw.Graph.Renderer, {

    name: 'xkcd',

    defaults: function($super) {

        return Rickshaw.extend( $super(), {
            unstack: true,
            fill: false,
            stroke: true
        } );
    },

    render: function() {
	var graph = this.graph;
	var strokeWidth = 3;

	graph.vis.selectAll('*').remove();

	var element = graph.vis.selectAll("path")
	    .data(this.graph.stackedData)
	    .enter();

	// Get xinterp function
	var local_xinterp = xkcd_xinterp_on_graph(graph);

	var line = d3.svg.line()
	    .x( function(d) { return graph.x(d.x) } )
	    .y( function(d) { return graph.y(d.y) } )
	    .interpolate(local_xinterp).tension(this.tension);

	// For now skip background lines.  Line crossings
	// won't look as good.

	var nodes = element.append("svg:path")
	    .attr("d", line)
            .style("stroke-width", strokeWidth + "px")
            .style("fill", "none");

	var i = 0;
	graph.series.forEach( function(series) {
	    if (series.disabled) return;
	    series.path = nodes[0][i++];
	    this._styleSeries(series);
	}, this );
    }

} );
