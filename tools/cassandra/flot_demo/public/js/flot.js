// Eventually we'll have various ways to mess with the
// interval on this and/or pause it
var ajaxGetInterval = false;
var plot = false;
var do_replot = true;
var do_grid_change = true;
var last_ts = false;
var timeRange = 0;    // the number of milliseconds to graph from the current time

var urlParams = {};
var uuid = '';
var graph_data = [];
var graph_url = '';

// Plot data is a hash of the data we've seen
var plot_data = {};

// This is the plot_data in flot format
var flot_data = [];

// This is the most recent list of labels
var old_labels = [];

var flot_opts = {
  series: {
    points: { show: true },
    lines: { show: true }
  },
  grid: {
    hoverable: true
  },
  xaxis: {
    mode: "time",
  },
  yaxis: {
  },
  zoom: {
    interactive: true
  },
  selection: {
    mode:"x"
  }
};

function refreshDropDowns() {
  uuid = $("#hostname_ddl").val();
  $.ajax({
    method: "get",
    url: "/statNames?uuid=" + uuid,
    dataType: 'json',
    success:function(statNames, status) {
      // clear the drop down
      $("select#statNameDdl").find('option').remove();
      // add the new options
      for( i = 0 ; i < statNames[uuid].length; i++ ) {
        name = statNames[uuid][i];
        $("select#statNameDdl").append("<option value=\"" + name + "\"" + ">" + name + "</option>")
      }
    },
    error:function(xhr, error, exception) {
      console.debug("AJAX failed on " + url + ": " + exception)
    }
  });
}

function hash_size(obj) {
  var size = 0, key;
  for (key in obj) {
    if (obj.hasOwnProperty(key)) size++;
  }
  return size;
};

// Create and Plot Data
function clearPlotData() {
  plot_data = {};
  flot_data = [];
  graph_data = [];
  last_ts = false;
  do_grid_change = true;
  if(plot) {
    plot.draw();
  }
}

// Add data to plot_data, update as new samples come in.
// Flot data is in format:
// [
//    { "label": "series1", "data": [[ t1, data1], [t2, data2]...] },
//    { "label": "series2", "data": [[ t1, data1], [t2, data2]...] }
// ]
//
// plot_data is simpler:
// {
//   "series1": { "t1": data1, "t2": data2 },
//   "series2": { "t1": data1, "t2": data2 }
// }
//
function mergePlotData(newData) {
  var statName;
  console.debug("Merging plot data...");

  // For each stat name
  for(statName in newData) {
    console.debug("Stat name: " + statName);
    if(!newData.hasOwnProperty(statName)) { next; }
    console.debug("Stat name " + statName + ", points: " + hash_size(newData[statName]));
    // Add new series to plot_data if it isn't there
    if(!plot_data[statName]) {
      plot_data[statName] = {};
      flot_data_series = { "label": statName, "data": [] };
      flot_data.push(flot_data_series);
      plot_data[statName].flot_data = flot_data_series.data;
      // The labels changed, redraw
      do_grid_change = true;
    }

    var oldSeries = plot_data[statName];
    var flotData = plot_data[statName].flot_data;
    var newPoints = newData[statName];
    var ts;

    for(ts in newPoints) {
      if(!newPoints.hasOwnProperty(ts)) { next; }
      var point = newPoints[ts];
      if(!oldSeries[ts]) {
        flotData.push([ Math.round(ts / 1000.0), point.value ])
        oldSeries[ts] = point.value;
      }
      if(!last_ts || ts > last_ts) {
        last_ts = Math.round(ts / 1000);    // ts is in microseconds
      }
    }
  }

  console.debug("Done merging plot data");
}

function drawWithData(theData) {
  var placeholder = $("#placeholder");

  graph_data = theData;
  
  // Start with empty data, schedule an AJAX update
  if(do_replot) {
    plot = $.plot(placeholder, theData, flot_opts);
    do_replot = false;
  } else {
    plot.setData(theData);
    plot.draw();
  }

  if(do_grid_change) {
    plot.setupGrid();
    do_grid_change = false;
  }

  // little helper for taking the repetitive work out of placing panning arrows
  function addArrow(dir, right, bottom, offset) {
    $('<img class="button" src="arrow-' + dir + '.gif" style="right:' + right + 'px;bottom:' + bottom + 'px">').appendTo(placeholder).click(function (e) {
      e.preventDefault();
      plot.pan(offset);
    });
  }

  addArrow('left', 55, 60, { left: -100 });
  addArrow('right', 25, 60, { left: 100 });
  addArrow('up', 40, 75, { top: -100 });
  addArrow('down', 40, 45, { top: 100 });

  $("#full_refresh").click(function() { updateGraphData(true); });
  $("#replot").click(function() { do_replot = true; updateGraphData(false); });
}

// Replaces the 'start' and 'end' query string params with an update to date
// timestamp that uses the current system time and the 'timeRange' value
function replaceStartAndEndTimes(url, range) {
  var now_ts = getEndTime();
  var start_ts = getStartTime(now_ts);
  var params = url.split("?")[1].split("&");
  var retval = url.split("?")[0] + "?";
  for(i = 0; i < params.length; i++) {
    var param_name = params[i].split("=")[0];
    if(param_name != "start" && param_name != "end") {
      retval += params[i] + "&";
    }
  }
  retval += "start="+start_ts+"&end="+now_ts;
  return retval;
}

function getStartTime(endTime) {
  return endTime - timeRange;
}

function getEndTime() {
  return (new Date()).getTime();
}

function updateGraphData(fullUpdate) {
  var now_ts = getEndTime();
  var start_ts = getStartTime(now_ts);

  clearPlotData();

  // Query for two minutes later than now.  Normally
  // there shouldn't be any data, but this (more than)
  // accounts for clock skew, request delay and whatnot.
  now_ts += 2 * 60 * 1000;

  if(graph_url.length == 0) {
    url = '/data_proxy/stat/json?start=' + start_ts + '&end=' + now_ts;
    url += '&uuid=' + uuid;
  } else {
    url = replaceStartAndEndTimes(graph_url, timeRange);
  }

  drawGraph(url);
}

function drawGraph(url) {
  graph_url = url;
  clearPlotData();
  var q = $.ajax({
    method : 'get',
    url : graph_url,
    dataType : 'json',
    success: function(data, status) {
      mergePlotData(data);
      drawWithData(flot_data);
      if(plot) {
        plot.draw();
      }
    },
    error: function (xhr, error, exception) {
      console.debug("AJAX failed on " + url + ": " + exception);
    }
  });
}

// Create and Show Tooltips
function showTooltip(x, y, contents) {
  $('<div id="tooltip">' + contents + '</div>').css( {
    position: 'absolute',
    display: 'none',
    top: y + 5,
    left: x + 5,
    border: '1px solid #fdd',
    padding: '2px',
    'background-color': '#fee',
    opacity: 0.80
  }).appendTo("body").fadeIn(200);
}

// Sets the timeRange value and updates the graph with the current time range data
function changeTimeRange(range) {
  timeRange = range;
  graph_url = replaceStartAndEndTimes(graph_url, timeRange);
  drawGraph(graph_url);
}

// Document ready
$(function () {

  (function () {
    var e;
    var a = /\+/g;  // Regex for replacing addition symbol with a space
    var r = /([^&=]+)=?([^&]*)/g;
    var d = function (s) { return decodeURIComponent(s.replace(a, " ")); };
    var q = window.location.search.substring(1);
    while (e = r.exec(q))
      urlParams[d(e[1])] = d(e[2]);
  })();

  // Add the interaction for the drop downs
  $("select#hostname_ddl").change(function() {
    refreshDropDowns();
    var now = new Date();
    var now_ts = now.getTime();
    var start_ts;
    start_ts = now_ts - (24 * 60 * 60 * 1000);
    uuid = $("#hostname_ddl").val();
    drawGraph("/data_proxy/stat/json?start="+start_ts+"&end="+now_ts+"&uuid="+uuid);
  });

  $("#placeholder").bind("plothover", function (event, pos, item) {
    var previousPoint = null;
    if ($("#enableTooltip:checked").length > 0) {
      if (item) {
        if (previousPoint != item.dataIndex) {
          previousPoint = item.dataIndex;
          $("#tooltip").remove();
          var x = new Date(item.datapoint[0]).toUTCString();
          var y = item.datapoint[1].toFixed(2);
          showTooltip(item.pageX, item.pageY,
                      item.series.label + " of " + x + " = " + y);
        }
      }
      else {
        $("#tooltip").remove();
        previousPoint = null;
      }
    }
  });

  // each time the stats drop down changes, attempt to redraw the graph
  $("select#statNameDdl").change(function() {
    clearPlotData();
    statName = $("select#statNameDdl").val();
    var now = new Date();
    var now_ts = now.getTime();
    var start_ts;
    start_ts = now_ts - (24 * 60 * 60 * 1000);
    clearInterval(ajaxGetInterval);
    ajaxGetInterval = setInterval(function() { updateGraphData(false); }, 10 * 1000)
    stat_url = "/data_proxy/stat/json?start="+start_ts+"&end="+now_ts+"&uuid="+uuid;
    if(statName != "All") {
      stat_url += "&name="+statName;
    }
    drawGraph(stat_url);
  });

  // register the binding for zooming
  $("#placeholder").bind("plotselected", function (event, ranges) {
    plot = $.plot($("#placeholder"), graph_data,
              $.extend(true, {}, flot_opts, {
                xaxis: { min: ranges.xaxis.from, max: ranges.xaxis.to }
           }));
  });

  $("span#oneHour").click(function() {
    changeTimeRange(60*60*1000);
  });
  $("span#threeHour").click(function() {
    changeTimeRange(3*60*60*1000);
  });
  $("span#sixHour").click(function() {
    changeTimeRange(6*60*60*1000);
  });
  $("span#twelveHour").click(function() {
    changeTimeRange(12*60*60*1000);
  });
  $("span#day").click(function() {
    changeTimeRange(24*60*60*1000);
  });
  $("span#week").click(function() {
    changeTimeRange(7*24*60*60*1000);
  });

  refreshDropDowns();
  // Every 10 seconds do a get-recent
  ajaxGetInterval = setInterval(function() { updateGraphData(false); }, 10 * 1000)
  updateGraphData(true);
  changeTimeRange(60*60*1000);
});

