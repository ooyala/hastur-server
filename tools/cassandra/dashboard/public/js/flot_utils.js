
function HasturFlot(parentElementId, containerId, opts) {
  // Eventually we'll have various ways to mess with the
  // interval on this and/or pause it
  var ajaxGetInterval = false;
  var plot = false;
  var do_replot;
  var do_grid_change = true;
  var last_ts = false;
  var timeRange = 0;    // the number of milliseconds to graph from the current time
  var uuid = '';
  var graph_data = [];
  var graph_url = '';
  var plot_data = {}; // Plot data is a hash of the data we've seen
  var flot_data = []; // This is the plot_data in flot format
  var old_labels = []; // This is the most recent list of labels
  var flot_opts;

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
  
  this.init = function() {
    if(opts) {
      var key;
      for(key in opts) {
        if( opts.hasOwnProperty(key) ) {
          this[key] = opts[key];
        }
      }
    } else {
      this.flot_opts = {
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
    }
    this.do_replot = true;
    this.setupDiv();         // setup the HasturFlot components
    this.populateHostnames();
  }

  this.populateHostnames = function () {
    var hostnameDdl = this.hostnameDdl;
    var hf = this;
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

        hf.setupListeners();
        hf.refreshDropDowns();            // update the statname drop downs
        // Every 10 seconds do a get-recent
        hf.ajaxGetInterval = setInterval(function() { hf.updateGraphData(false); }, 10 * 1000)
        hf.updateGraphData(true);
        
        // only default to 1h if the opts was not set
        if(opts) {
          hf.changeTimeRange(hf.timeRange);
        } else {
          hf.changeTimeRange(60*60*1000);
        }
      },
      error:function(xhr, error, exception) {
        console.debug("AJAX failed on " + url + ": " + exception)
      }
    });

  }

  this.setupDiv = function () {
    var t = (new Date()).getTime();
    this.hostnameDdl = "hostnameDdl-" + t;
    this.statNameDdl = "statNameDdl-" + t;
    this.graph = "graphArea-" + t;
    this.oneHour = "oneHour-" + t;
    this.threeHour = "threeHour-" + t;
    this.sixHour = "sixHour-" + t;
    this.twelveHour = "twelveHour-" + t;
    this.day = "day-" + t;
    this.threeDay = "threeDay-" + t;
    this.timeContainer = "timeContainer-" + t;
    this.container = containerId;
    // parent adds container
    getElement(parentElementId).append("<li><div id='"+ containerId +"'></div></li>");

    // container adds time range options, drop downs
    getElement(this.container).append("<span class='headerSpan'>Hosts</span>");
    getElement(this.container).append("<select id='"+ this.hostnameDdl +"'></select>");
    getElement(this.container).append("<span class='headerSpan'>Stats</span>");
    getElement(this.container).append("<select id='"+ this.statNameDdl +"'></select>");
    getElement(this.container).append("<div id='" + this.timeContainer + "'></div>");

    // timeContainer adds time range options
    getElement(this.timeContainer).append("<span class='headerSpan'>Zoom</span>");
    getElement(this.timeContainer).append("<span class='headerSpan' id='" + this.oneHour + "' style='color:lightblue'>1h</span>");
    getElement(this.timeContainer).append("<span class='headerSpan' id='" + this.threeHour + "' style='color:lightblue'>3h</span>");
    getElement(this.timeContainer).append("<span class='headerSpan' id='" + this.sixHour + "' style='color:lightblue'>6h</span>");
    getElement(this.timeContainer).append("<span class='headerSpan' id='" + this.twelveHour + "' style='color:lightblue'>12h</span>");
    getElement(this.timeContainer).append("<span class='headerSpan' id='" + this.day + "' style='color:lightblue'>1d</span>");
    getElement(this.timeContainer).append("<span class='headerSpan' id='" + this.threeDay + "' style='color:lightblue'>3d</span>");

    // container adds graph
    getElement(this.container).append("<div id='" + this.graph + "'></div>");
    getElement(this.graph).css("width", "500px");
    getElement(this.graph).css("height", "300px");
  }

  this.setupListeners = function () {
    var hf = this;
    getElement(this.oneHour).click( function() {
      hf.changeTimeRange(60*60*1000);
    });
    getElement(this.threeHour).click( function() {
      hf.changeTimeRange(3*60*60*1000);
    });
    getElement(this.sixHour).click( function() {
      hf.changeTimeRange(6*60*60*1000);
    });
    getElement(this.twelveHour).click( function() {
      hf.changeTimeRange(12*60*60*1000);
    });
    getElement(this.day).click( function() {
      hf.changeTimeRange(24*60*60*1000);
    });
    getElement(this.threeDay).click( function() {
      hf.changeTimeRange(3*24*60*60*1000);
    });

    // Add the interaction for the drop downs
    getElement(this.hostnameDdl).change(function() {
      hf.refreshDropDowns();
      hf.uuid = getElement(hf.hostnameDdl).val();
      var url = "/data_proxy/stat/json?uuid="+hf.uuid;
      url = replaceStartAndEndTimes(url, hf.timeRange);
      hf.drawGraph( url );
    });

//    graph.bind("plothover", function (event, pos, item) {
//      var previousPoint = null;
//      if ($("#enableTooltip:checked").length > 0) {
//        if (item) {
//          if (previousPoint != item.dataIndex) {
//            previousPoint = item.dataIndex;
//            $("#tooltip").remove();
//            var x = new Date(item.datapoint[0]).toUTCString();
//            var y = item.datapoint[1].toFixed(2);
//            showTooltip(item.pageX, item.pageY,
//                        item.series.label + " of " + x + " = " + y);
//          }
//        }
//        else {
//          $("#tooltip").remove();
//          previousPoint = null;
//        }
//      }
//    });
    
    // each time the stats drop down changes, attempt to redraw the graph
    getElement(this.statNameDdl).change(function() {
      hf.clearPlotData();
      var statName = getElement(hf.statNameDdl).val();
      var stat_url = "/data_proxy/stat/json?uuid="+hf.uuid;
      stat_url = replaceStartAndEndTimes(stat_url, hf.timeRange);
      if(statName != "All" && statName != undefined) {
        stat_url += "&name="+statName;
      }
      hf.drawGraph(stat_url);
    });

    // register the binding for zooming
    getElement(this.graph).bind("plotselected", function (event, ranges) {
      this.plot = $.plot(getElement(this.graph), this.graph_data,
                $.extend(true, {}, this.flot_opts, {
                  xaxis: { max: ranges.xaxis.to },
                  yaxis: { max: ranges.yaxis.to }
             }));
    });
  }

  function getElement( elementId ) {
    return $("#"+elementId);
  }

  this.refreshDropDowns = function () {
    var uuid = getElement(this.hostnameDdl).val();
    this.uuid = uuid;
    var statNameDdl = this.statNameDdl;
    var statUrl = "/statNames?uuid=" + uuid;
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
  this.clearPlotData = function() {
    this.plot_data = {};
    this.flot_data = [];
    this.graph_data = [];
    this.last_ts = false;
    this.do_grid_change = true;
    if(this.plot && this.plot.draw) {
      this.plot.draw();
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
  this.mergePlotData = function (newData) {
    var statName;
    //console.debug("Merging plot data...");

    // For each stat name
    for(statName in newData) {
      //console.debug("Stat name: " + statName);
      if(!newData.hasOwnProperty(statName)) { next; }
      //console.debug("Stat name " + statName + ", points: " + hash_size(newData[statName]));
      // Add new series to plot_data if it isn't there
      if(!this.plot_data[statName]) {
        this.plot_data[statName] = {};
        this.flot_data_series = { "label": statName, "data": [] };
        this.flot_data.push(this.flot_data_series);
        this.plot_data[statName].flot_data = this.flot_data_series.data;
        // The labels changed, redraw
        this.do_grid_change = true;
      }

      var oldSeries = this.plot_data[statName];
      var flotData = this.plot_data[statName].flot_data;
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

    //console.debug("Done merging plot data");
  }

  this.drawWithData = function(theData) {
    this.graph_data = theData;
    
    // Start with empty data, schedule an AJAX update
    if(this.do_replot) {
      this.plot = $.plot(getElement(this.graph), theData, this.flot_opts);
      this.do_replot = false;
    } else {
      this.plot.setData(theData);
      this.plot.draw();
    }

    if(this.do_grid_change) {
      this.plot.setupGrid();
      this.do_grid_change = false;
    }

    // little helper for taking the repetitive work out of placing panning arrows
    function addArrow(dir, right, bottom, offset) {
      $('<img class="button" src="arrow-' + dir + '.gif" style="right:' + right + 'px;bottom:' + bottom + 'px">').appendTo(graph).click(function (e) {
        e.preventDefault();
        this.plot.pan(offset);
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
    var start_ts = getStartTime(now_ts, range);
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

  function getStartTime(endTime, range) {
    return endTime - range;
  }

  function getEndTime() {
    return (new Date()).getTime();
  }

  this.updateGraphData = function(fullUpdate) {
    var now_ts = getEndTime();
    var start_ts = getStartTime(now_ts, this.timeRange);

    this.clearPlotData();

    // Query for two minutes later than now.  Normally
    // there shouldn't be any data, but this (more than)
    // accounts for clock skew, request delay and whatnot.
    now_ts += 2 * 60 * 1000;
    var url;
    if(this.graph_url == undefined || this.graph_url.length == 0) {
      url = '/data_proxy/stat/json?start=' + start_ts + '&end=' + now_ts;
      url += '&uuid=' + this.uuid;
    } else {
      url = replaceStartAndEndTimes(this.graph_url, this.timeRange);
    }

    this.drawGraph(url);
  }

  this.drawGraph = function(url) {
    this.graph_url = url;
    console.debug(url + " => " + this.timeRange);
    this.clearPlotData();
    var hf = this;
    var q = $.ajax({
      method : 'get',
      url : url,
      dataType : 'json',
      success: function(data, status) {
        hf.mergePlotData(data);
        hf.drawWithData(hf.flot_data);
        if(hf.plot) {
          hf.plot.draw();
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
  this.changeTimeRange = function (range) {
    this.timeRange = range;
    this.highlightRange(range);
    this.graph_url = replaceStartAndEndTimes(this.graph_url, this.timeRange);
    this.drawGraph(this.graph_url);
  }

  // Colors each of the range spans a certain color
  this.highlightRange = function(range) {
    highlightRangeSpan(this.oneHour, 60*60*1000, range);
    highlightRangeSpan(this.threeHour, 3*60*60*1000, range);
    highlightRangeSpan(this.sixHour, 6*60*60*1000, range);
    highlightRangeSpan(this.twelveHour, 12*60*60*1000, range);
    highlightRangeSpan(this.day, 24*60*60*1000, range);
    highlightRangeSpan(this.threeDay, 3*24*60*60*1000, range);
  }

  // Colors one range span a certain color depending on if the range matches the span's value
  function highlightRangeSpan(element_id, spanRange, range) {
    var color;
    if(range == spanRange) {
      color = "darkolivegreen";
    } else {
      color = "lightblue";
    }
    getElement(element_id).css("color", color);
  }
}

// Document ready
var urlParams = {};
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

