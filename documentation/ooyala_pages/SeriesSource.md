SeriesSource queries Hastur repeatedly and returns the results.  Here are some advantages of SeriesSource over raw Ajax on Hastur URLs:

* Graphing library bindings - SeriesSource hooks up to [Hastur/SeriesRickshaw](SeriesRickshaw) and [Hastur/SeriesSparkline](SeriesSparkline) automatically.
* Incremental query.  You don't have to query everything, every time.  This gives better framerates, better remote/VPN performance and better server efficiency.
* Storage/caching - you can update your graph from the cached data, allowing refresh on window resize without re-querying the server.
* Transformations - SeriesSource includes a TransformSource to transform data entries automatically.
* Flexibility - specify defaults via query params for all queries so your dashboard can easily change the Hastur host, the API version or the time period without changing your JS code.
* Version independence.  SeriesSource builds Hastur URLs in a forward-compatible way.

You can always do it all yourself, but why?

# How To Use

SeriesSource is in the hastur-portal repository, as js/hastur/series_source.js.  You can copy or link it.  We're currently not doing versions and releases, making linking it more dangerous (can change) but more useful (you can get fixes and upgrades).

Let me know if you have a use case requiring versioning, which will eventually be important but for now is more work.

# SeriesSource Options

Hastur options:

* uuid: UUID(s) to query, comma-separated list
* name: Name(s) to query, comma-separated list
* type: Type(s) to query, comma-separated list
* label: Labels(s) to query, in the form: `foo:bar,!baz,quux:abc*`
* fun: [Hastur/Retrieval Functions](Retrieval Functions) to use, such as `hostname(compound(:p50,bin(400)))`. Default: hostname()
* format: value, message, rollup, etc. -- corresponds to "kind" in API v2
* ago: how far back to query, such as one_day or five_minutes
* start: Starting timestamp, in microseconds since the Unix Epoch
* end: Ending timestamp, in microseconds since the Unix Epoch
* rollup: Rollup period, such as "five_minutes"

Note: most of these are the same as the Hastur options in v2, but "format" and "rollup" are slightly different.  May need to change soon.

SeriesSource options that aren't simply Hastur options:

* autorefresh: if present, should be a number of milliseconds.  15000 means "re-query every 15 seconds".  Default: false
* incremental: whether to repeatedly query and put together the results naively.  DO NOT USE when using retrieval functions like bin() or segment() where a "last five minutes" query may look completely different than a full-period query.  Default: false for now, true eventually.
* host: the host to query, such as "hastur.ooyala.com".  Default: hastur.ooyala.com.
* v2: whether to use API v2 for the queries.  Default: false.
* accessor: if the query returns a rollup, use this to get which field.  Default: "average".

The "v2" parameter should give way to a "version" parameter, but hasn't yet.

# TransformSource Options

To create a Transform source, first create the SeriesSource that it transforms.  Then, create the TransformSource with a JS object, which specifies the source and one transform:

~~~
var trans_func = function(value) { return value * 2.0 }
var transform_source = new Hastur.TransformSource({ source: my_series_source, value_transform: trans_func })
~~~

Specify exactly one of value_transform, subseries_transform or series_transform.

* A Value transform is run for each element.
* A Series transform takes the entire query result as input, and returns another one as output.
* A Subseries transform is mapped across the top-level query.  Its input is of the form `{ name: subseries_name, data: [ {t1: v1}, {t2: v2} ] }` where t1 and t2 are timestamps as strings, and v1 and v2 are their corresponding values.  The full series is an array of those subseries elements.

# When To Use SeriesSource

Usually you don't need to use SeriesSource explicitly.  Pass a JavaScript object with the SeriesSource options to SeriesRickshaw or SeriesSparkline and those libraries will create a SeriesSource for you, automatically.  See your graphing library binding documentation for details.

However, if you want to:

* transform the results (see TransformSource)
* share one Hastur query among multiple graphs, optionally transformed
* write your own graph library binding
* use the data directly, with no graph library binding

then you'll need to explicitly make a SeriesSource.  Then you can pass it to SeriesRickshaw or SeriesSparkline directly, or use it yourself.

# To Fix

* Several parameters like format, kind and rollup don't match to the Hastur retrieval API as well as we'd hope, and/or need fixing in the retrieval API
* The v2 parameter should become "version".
* Need more incremental-query parameters to update rolled-up stuff.  "Segment" is an attempt in this direction, but isn't done.
