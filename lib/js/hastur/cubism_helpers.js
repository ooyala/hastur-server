var hastur = hastur || {};

hastur.cubism = {
  cpu: function(context, host) {
    return [
      { title: host + ": cpu usage   ",
        metric: hc.metric(host, 'linux.proc.stat',
          'derivative(:shift,unrollover(compound(:cpu.user,:cpu.system,:cpu.wait)))') }
    ];
  },

  net: function(context, host, iface) {
    return [
      { title: host + ": rx bytes on " + iface + "   ",
        metric: hc.metric( host, 'linux.proc.net.dev',
        'derivative(:shift,unrollover(compound(:'+iface+'.rx_bytes)))' ) },
      { title: host + ": tx bytes on " + iface + "   ",
        metric: hc.metric( host, 'linux.proc.net.dev',
        'derivative(:shift,unrollover(compound(:'+iface+'.tx_bytes)))' ) }
    ];
  },

  disk: function(context, host, devices) {
    var read = devices.map(function (item) { return ':' + item + ".sectors_read" }).join();
    var write = devices.map(function (item) { return ':' + item + ".sectors_write" }).join();

    return [
      { title: host + ": disk read " + devices.join(", ") + "   ",
        metric: hc.metric( host, 'linux.proc.diskstats',
        'scale(512,derivative(:shift,unrollover(compound('+ read +'))))') },
      { title: host + ": disk write " + devices.join(", ") + "   ",
        metric: hc.metric( host, 'linux.proc.diskstats',
       'scale(-512,derivative(:shift,unrollover(compound('+ write +'))))') }
    ];
  }
};

