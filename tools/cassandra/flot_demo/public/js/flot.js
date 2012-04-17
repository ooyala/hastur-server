// Eventually we'll have various ways to mess with the
// interval on this and/or pause it
var ajaxGetInterval = false;
var plot = false;
var do_replot = true;
var do_grid_change = true;
var last_ts = false;

var urlParams = {};
var uuid = '';

// Plot data is a hash of the data we've seen
var plot_data = {};

// This is the plot_data in flot format
var flot_data = [];

// This is the most recent list of labels
var old_labels = [];

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

//  uuid = urlParams["uuid"];
  uuid = $("#hostname_ddl")[0].value;
  $("#hostname_span").html(uuid);

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
    last_ts = false;
    do_grid_change = true;
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

        if(!last_ts || ts > last_ts) { last_ts = ts; }
      }
    }

    console.debug("Done merging plot data");
  }

  function drawWithData(theData) {
    var placeholder = $("#placeholder");

    // Start with empty data, schedule an AJAX update
    if(do_replot) {
      plot = $.plot(placeholder, theData, {
        series: {
          points: { show: true }
        },
        grid: {
          hoverable: true
        },
        xaxis: {
          mode: "time",
          zoomRange: null,
          panRange: null
        },
        yaxis: {
          zoomRange: null,
          panRange: null
        },
        zoom: {
          interactive: true
        },
        pan: {
          interactive: true
        }
      });

      do_replot = false;
    } else {
      plot.setData(theData);
      plot.draw();
    }

    if(do_grid_change) {
      plot.setupGrid();
      do_grid_change = false;
    }

    // Navigation - Panning and Zooming

    // add zoom out button 
    $('<div class="button" style="right:20px;bottom:100px">zoom out</div>').appendTo(placeholder).click(function (e) {
      e.preventDefault();
      plot.zoomOut();
    });

    // and add panning buttons

    // little helper for taking the repetitive work out of placing
    // panning arrows
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

  function updateGraphData(fullUpdate) {
    var now = new Date();
    var now_ts = now.getTime();
    var start_ts;

    if(fullUpdate) {
      console.debug("Clear plot data for full update");
      clearPlotData();
    }
    
    if(!last_ts || fullUpdate) {
      start_ts = now_ts - 24 * 60 * 60 * 1000;
    } else {
      start_ts = last_ts - 10 * 1000;  // Re-get last 10 seconds of data
    }

    // Query for two minutes later than now.  Normally
    // there shouldn't be any data, but this (more than)
    // accounts for clock skew, request delay and whatnot.
    now_ts += 2 * 60 * 1000;

    url = '/data_proxy/stat/json?start=' + start_ts + '&end=' + now_ts;
    url += '&uuid=' + uuid

    var q = $.ajax({
       method: 'get',
       url : url,
       dataType : 'json',
       success: function(data, status) {
         mergePlotData(data);
         drawWithData(flot_data);
      },
      error: function (xhr, error, exception) {
         console.debug("AJAX failed on " + url + ": " + exception);
      }
    });
  }

  // Every 10 minutes do a full refresh
  setInterval(function() { updateGraphData(true); }, 10 * 60 * 1000)

  // Every 2 seconds do a get-recent
  ajaxGetInterval = setInterval(function() { updateGraphData(false); }, 2 * 1000)
  updateGraphData(true);

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
  var previousPoint = null;
  $("#placeholder").bind("plothover", function (event, pos, item) {
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

});
