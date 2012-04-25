
function HasturFlot(parentElementId, containerId) {
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
  var plot_data = {}; // Plot data is a hash of the data we've seen
  var flot_data = []; // This is the plot_data in flot format
  var old_labels = []; // This is the most recent list of labels
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

  // ID of the HTML components in the HasturFlot widget
  var hostnameDdl;
  var statNameDdl;
  var graph;
  var oneHour;
  var threeHour;
  var sixHour;
  var twelveHour;
  var day;
  var threeDay;
  var container;
  var timeContainer;

  setupDiv();         // setup the HasturFlot components
  setupListeners();
  init();

  function init() {
    populateHostnames();
  }

  function populateHostnames() {
    $.ajax({
      method: "get",
      url: "/hostnames",
      dataType: 'json',
      success:function(hosts, status) {
        // clear the drop down
        getElement(hostnameDdl).find('option').remove();
        // add the new options
        for(var u in hosts) {
          if(hosts.hasOwnProperty(u)) {
            getElement(hostnameDdl).append("<option value=\"" + u + "\"" + ">" + hosts[u] + "</option>")
          }
        }

        refreshDropDowns();            // update the statname drop downs
        // Every 10 seconds do a get-recent
        ajaxGetInterval = setInterval(function() { updateGraphData(false); }, 10 * 1000)
        updateGraphData(true);
        changeTimeRange(60*60*1000);
      },
      error:function(xhr, error, exception) {
        console.debug("AJAX failed on " + url + ": " + exception)
      }
    });

  }

  function setupDiv() {
    t = (new Date()).getTime();
    hostnameDdl = "hostnameDdl-" + t;
    statNameDdl = "statNameDdl-" + t;
    graph = "graph-" + t;
    oneHour = "oneHour-" + t;
    threeHour = "threeHour-" + t;
    sixHour = "sixHour-" + t;
    twelveHour = "twelveHour-" + t;
    day = "day-" + t;
    threeDay = "threeDay-" + t;
    container = containerId;
    timeContainer = "timeContainer-" + t;

    // parent adds container
    getElement(parentElementId).append("<li><div id='"+ containerId +"'></div></li>");

    // container adds time range options, drop downs
    getElement(container).append("<span class='headerSpan'>Hosts</span>");
    getElement(container).append("<select id='"+ hostnameDdl +"'></select>");
    getElement(container).append("<span class='headerSpan'>Stats</span>");
    getElement(container).append("<select id='"+ statNameDdl +"'></select>");
    getElement(container).append("<div id='" + timeContainer + "'></div>");

    // timeContainer adds time range options
    getElement(timeContainer).append("<span class='headerSpan'>Zoom</span>");
    getElement(timeContainer).append("<span class='headerSpan' id='" + oneHour + "' style='color:lightblue'>1h</span>");
    getElement(timeContainer).append("<span class='headerSpan' id='" + threeHour + "' style='color:lightblue'>3h</span>");
    getElement(timeContainer).append("<span class='headerSpan' id='" + sixHour + "' style='color:lightblue'>6h</span>");
    getElement(timeContainer).append("<span class='headerSpan' id='" + twelveHour + "' style='color:lightblue'>12h</span>");
    getElement(timeContainer).append("<span class='headerSpan' id='" + day + "' style='color:lightblue'>1d</span>");
    getElement(timeContainer).append("<span class='headerSpan' id='" + threeDay + "' style='color:lightblue'>3d</span>");

    // container adds graph
    getElement(container).append("<div id='" + graph + "'></div>");
    getElement(graph).css("width", "500px");
    getElement(graph).css("height", "300px");
  }

  function setupListeners() {
    // Add the interaction for the drop downs
    getElement(hostnameDdl).change(function() {
      refreshDropDowns();
      uuid = getElement(hostnameDdl).val();
      url = "/data_proxy/stat/json?uuid="+uuid;
      url = replaceStartAndEndTimes(url, timeRange);
      drawGraph( url );
    });

/*
    graph.bind("plothover", function (event, pos, item) {
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
*/
    // each time the stats drop down changes, attempt to redraw the graph
    getElement(statNameDdl).change(function() {
      clearPlotData();
      statName = getElement(statNameDdl).val();
      stat_url = "/data_proxy/stat/json?uuid="+uuid;
      stat_url = replaceStartAndEndTimes(stat_url, timeRange);
      if(statName != "All") {
        stat_url += "&name="+statName;
      }
      drawGraph(stat_url);
    });

    // register the binding for zooming
    getElement(graph).bind("plotselected", function (event, ranges) {
      plot = $.plot(getElement(graph), graph_data,
                $.extend(true, {}, flot_opts, {
                  xaxis: { max: ranges.xaxis.to },
                  yaxis: { max: ranges.yaxis.to }
             }));
    });

    getElement(oneHour).click(function() {
      changeTimeRange(60*60*1000);
    });
    getElement(threeHour).click(function() {
      changeTimeRange(3*60*60*1000);
    });
    getElement(sixHour).click(function() {
      changeTimeRange(6*60*60*1000);
    });
    getElement(twelveHour).click(function() {
      changeTimeRange(12*60*60*1000);
    });
    getElement(day).click(function() {
      changeTimeRange(24*60*60*1000);
    });
    getElement(threeDay).click(function() {
      changeTimeRange(3*24*60*60*1000);
    });
  }

  function getElement( elementId ) {
    return $("#"+elementId);
  }

  function refreshDropDowns() {
    uuid = getElement(hostnameDdl).val();
    statUrl = "/statNames?uuid=" + uuid;
    $.ajax({
      method: "get",
      url: statUrl,
      dataType: 'json',
      success:function(statNames, status) {
        // clear the drop down
        getElement(statNameDdl).find('option').remove();
        // add the new options
        for( i = 0 ; i < statNames[uuid].length; i++ ) {
          name = statNames[uuid][i];
          getElement(statNameDdl).append("<option value=\"" + name + "\"" + ">" + name + "</option>")
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
    graph_data = theData;
    
    // Start with empty data, schedule an AJAX update
    if(do_replot) {
      plot = $.plot(getElement(graph), theData, flot_opts);
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
      $('<img class="button" src="arrow-' + dir + '.gif" style="right:' + right + 'px;bottom:' + bottom + 'px">').appendTo(graph).click(function (e) {
        e.preventDefault();
        plot.pan(offset);
      });
    }

    addArrow('left', 55, 60, { left: -100 });
    addArrow('right', 25, 60, { left: 100 });
    addArrow('up', 40, 75, { top: -100 });
    addArrow('down', 40, 45, { top: 100 });
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
    highlightRange(range);
    graph_url = replaceStartAndEndTimes(graph_url, timeRange);
    drawGraph(graph_url);
  }

  // Colors each of the range spans a certain color
  function highlightRange(range) {
    highlightRangeSpan(oneHour, 60*60*1000, range);
    highlightRangeSpan(threeHour, 3*60*60*1000, range);
    highlightRangeSpan(sixHour, 6*60*60*1000, range);
    highlightRangeSpan(twelveHour, 12*60*60*1000, range);
    highlightRangeSpan(day, 24*60*60*1000, range);
    highlightRangeSpan(threeDay, 3*24*60*60*1000, range);
  }

  // Colors one range span a certain color depending on if the rnage matches the span's value
  function highlightRangeSpan(element_id, spanRange, range) {
    if(range == spanRange) {
      color = "darkolivegreen";
    } else {
      color = "lightblue";
    }
    getElement(element_id).css("color", color);
  }

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

});

var graphArray = new Array();

function addGraph(parentId) {
  graphArray[ graphArray.length ] = new HasturFlot(parentId, "graph-" + (graphArray.length+1));
}

