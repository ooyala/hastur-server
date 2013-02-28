You can find aggregation API docs [on yard-doc.ooyala](http://yard-doc.ooyala.com/docs/hastur-server/Hastur/Aggregation).

# Getting Started

You'll want to start playing with a Hastur query to get the feel of these.  I recommend beginning with curl on the command line.

Here's one query to start with:

    curl "http://hastur.ooyala.com/api/name/hastur.router.messages.forwarded/value"

This will give you a set of counts of how many messages various Hastur servers have forwarded in the last five minutes.  We'll start with this query and modify.

You'll also want to read about the retrieval service -- all of these functions process data returned via Hastur retrieval, so it's useful to understand its usage.  Here are [Retrieval v1 docs](http://yard-doc.ooyala.com/docs/hastur-server/Hastur/Service/Retrieval) and [Hastur/Retrieval Service v2](Retrieval Service v2) docs.  These functions can be used with either unless specifically called out below or in the documentation.

# Data Types

The retrieval functions support two data types -- a Hastur series and a Hastur rollup series.

## Hastur Series

A Hastur series is a multilevel hash mapping UUIDs to stat names to timestamps to values.  Here's an example:

    {
      "47e88150-0102-0130-e57d-64ce8f3a9dc2": {
        "hastur.cassandra.schema.raw_get_all.rows": {
          "1354901523575000": 0
        },
        "hastur.cassandra.schema.raw_get_all.rows_queried": {
          "1354901523575000": 2
        },
        "hastur.cassandra.schema.raw_get_all.columns": {
          "1354901523575000": 0
        },
        "hastur.cassandra.schema.raw_get_all.time": {
          "1354901523575000": 40000
        }
      }
    }

If you query across multiple sending hosts and/or multiple statistic names, you'll see multiple entries for each in the Hastur series data.

This is also the raw data format of an un-transformed Hastur query.

## Hastur Rollup Series

The Hastur rollup series is the same structure, but instead of values for each timestamp, it has rollups.  Rollups are structured as follows:

    "1354901523622000": {
      "min": 96,
      "max": 100,
      "range": 4,
      "sum": 490,
      "count": 5,
      "first_ts": 1354901523622000,
      "last_ts": 1354901523626999,
      "elapsed": 5000,
      "interval": 5000,
      "stddev": 1.4142135623730951,
      "variance": 2.0,
      "average": 98.0,
      "p1": 96,
      "p5": 96,
      "p10": 96,
      "p25": 97,
      "p50": 98,
      "p75": 99,
      "p90": 100,
      "p95": 100,
      "p99": 100,
      "period": 1000.0,
      "jitter": 0.0
    },

We may add more quantities over time, but to explain these:

* average: the mean of the sample values
* stddev: standard deviation of sample values
* range: the highest value minus the lowest value within the interval
* sum: the sum of values over the interval
* stddev: standard deviation of values over the interval
* variance: variance of values over the interval - can be derived from stddev and vice-versa
* interval: the supplied interval length being rolled up, if supplied on rollup
* p1,p5...,p99: percentile values.  "p1" is the first-percentile sample value, "p50" is the median, "p99" is the 99th percentile.  Each corresponds to a single sample value from those queried.
* period: estimated average time between samples during this period.  Useful for heartbeats.
* jitter: estimated variance in sample time during this period -- the "0" above only happens if all samples are exactly equally spaced.  Useful for heartbeats.

# Syntax

The retrieval server uses a slightly odd syntax -- it doesn't execute arbitrary Ruby code, and in fact its parser is very limited.

I'll describe how to successfully write a (relatively) complex query and you can explore further if you like.

Strings are written like Ruby symbols -- :average, :p50, :first and so on.  Numbers are written exactly as they are in Ruby or other programming languages.  The other valid tokens are true, false, null (or nil) and function names, which are bare.

The query is basically a pipeline, where each level processes the results of the previous level.  They are written from left (outermost) to right (innermost).  We'll write them nested here though that's not technically required.

Each level's final argument, implicitly or explicitly, will be the next level of the pipeline.  For instance:

    fun="segment(5000,hostname(compound(:average)))"

Note that the argument to segment (5000) comes *before* the next retrieval function (hostname).  This is mandatory in the current parser.

If a given function has no other function as its final argument, its last argument is implicitly the original query.  In this case, "compound(:average)" has one non-function argument (:average).  This can also happen when you give no non-function arguments to a given aggregation function:

    fun="hostname()"

In this case, the parentheses are optional.

# Frequently Used Functions

You'll very often want to use these:

## Hostname

`fun=hostname()` will map UUIDs in the output into DNS hostnames where Hastur knows them.  This is much better for human readers.

Compare the top-level keys of this query:

    curl "http://hastur.ooyala.com/api/name/hastur.router.messages.forwarded/value?fun=hostname()"

to this one:

    curl "http://hastur.ooyala.com/api/name/hastur.router.messages.forwarded/value"

The first gives only UUIDs like "8a6654e8-17ea-11e2-b1b2-d4ae52739678".  The second gives names like "cass-c2n1.sv2".

## Rollup

`fun=rollup()` will roll up the returned values.  Try this:

    curl "http://hastur.ooyala.com/api/name/hastur.router.messages.forwarded/value?fun=hostname(rollup())"

For each host, it will add a large structure with different calculated values:

    "hastur.router.messages.forwarded": {
      "1354572230858714": 3991,
      "1354572260863142": 4162,
      "1354572290870677": 3890,
      "1354572320880687": 4855,
      "1354572350907276": 3999,
      "1354572380925382": 4218,
      "1354572410925872": 3939,
      "1354572440926595": 4842,
      "1354572470927850": 3935,
      "1354572500930738": 4282
    },
    "hastur.router.messages.forwarded.rollup": {
      "min": 3890,
      "max": 4855,
      "range": 965,
      "sum": 42113,
      "count": 10,
      "first_ts": 1354572230858714,
      "last_ts": 1354572500930738,
      "elapsed": 270072024,
      "interval": 86400000000,
      "stddev": 341.6038787835993,
      "variance": 116693.21,
      "average": 4211.3,
      "p1": 3935,
      "p5": 3935,
      "p10": 3939,
      "p25": 3991,
      "p50": 4218,
      "p75": 4842,
      "p90": null,
      "p95": null,
      "p99": null,
      "period": 30008002.666666668,
      "jitter": 8470.078052900235
    }

Some of the values are pretty obvious -- first_ts is the first timestamp in the interval, for instance, and "interval" is how long between the first and last timestamp.  The stddev is the standard deviation, variance is the variance, "average" is the average and so on.  These are calculated from the initial data values, as shown in the previous examples and just before the rollup in this example.

The "p" values are percentiles -- "p1" is the first-percentile value, "p50" is the median, "p90" is the ninetieth percentile and so on.  Notice that the highest percentiles aren't defined if you don't have enough samples.  We plan to fix that bug, but right now we just round the offset in the array downward, giving the current results.

Rolling up in this way is not terribly efficient -- querying the rollup directly is much faster since it's precalculated.  But that assumes you want a timestamp-aligned rollup of the right length of time (given as five_minutes, one_hour and one_day), which you may not.

## Compound

The compound function is used to extract a field of a compound structure such as a rollup.  For instance, let's grab a rollup.  After that we'll get information from it.

First, the rollup itself:

    curl "http://hastur.ooyala.com/api/name/hastur.router.messages.forwarded/rollup?rollup_period=five_minutes&ago=one_hour"

The results will include bits like:

      "1354572900000000": {
        "min": 4045,
        "max": 4760,
        "range": 715,
        "sum": 8805,
        "count": 2,
        "first_ts": 1354572921023741,
        "last_ts": 1354572951047298,
        "elapsed": 30023557,
        "interval": 300000000,
        "stddev": 357.5,
        "variance": 127806.25,
        "average": 4402.5,
        "p1": 4760,
        "p5": 4760,
        "p10": 4760,
        "p25": 4760,
        "p50": null,
        "p75": null,
        "p90": null,
        "p95": null,
        "p99": null,
        "period": 30023557.0,
        "jitter": 0.0
      }

Now let's extract the sum over each period:

    curl "http://hastur.ooyala.com/api/name/hastur.router.messages.forwarded/rollup?rollup_period=five_minutes&ago=one_hour&fun=compound(:sum)"

Now you'll get just the sum for each five minute period for each host:

    "a458afb6-1968-11e2-98a8-782bcb75a6d9": {
      "hastur.router.messages.forwarded.sum": {
        "1354569600000000": 36880,
        "1354569900000000": 37321,
        "1354570200000000": 36758,
        "1354570500000000": 37116,
        "1354570800000000": 36908,
        "1354571100000000": 37416,
        "1354571400000000": 36730,
        "1354571700000000": 37154,
        "1354572000000000": 36597,
        "1354572300000000": 36978,
        "1354572600000000": 36778,
        "1354572900000000": 3884
      }
    },
    "8a6654e8-17ea-11e2-b1b2-d4ae52739678": {
      "hastur.router.messages.forwarded.sum": {
        "1354569600000000": 42580,
        "1354569900000000": 42844,
        "1354570200000000": 42255,
        "1354570500000000": 42753,
        "1354570800000000": 42276,
        "1354571100000000": 42865,
        "1354571400000000": 41896,
        "1354571700000000": 42554,
        "1354572000000000": 41833,
        "1354572300000000": 42685,
        "1354572600000000": 41798,
        "1354572900000000": 8805
      }
    }

You can similarly extract the average, the median and so on -- try without the "compound" statement to see what's available in the structure.

# Further docs

Aggregation functions are somewhat advanced, and operate very much in the context of the Hastur retrieval service.  Make sure you already understand the basics of the retrieval API first.

For more perspective and links to further documentation, please see the [Hastur/User Guide]().

# Legacy Docs

Getting counts:

~~~
curl "http://hastur.ooyala.com/api/name/p13n_api.production.experiments.client.find_variations.total_time/rollup?rollup_period=one_hour&ago=one_week&fun=hostname(compound(:count))"
~~~

~~~
curl "http://hastur.ooyala.com/api/name/p13n_api.production.experiments.client.find_variations.total_time/rollup?rollup_period=one_hour&ago=one_week&fun=hostname(compact(compound(:max)))"
~~~

Very simple reference:

* "compound" - extract a field from a rollup or other compound value
* "compact" - remove all nulls and non-numerics or convert them to a supplied value.

`fun=rollup()` will roll up the returned values with some good metrics.  Test via curl.  Here are some examples:

~~~
# TODO: update to API v2 when it hits production
/api/name/foo.*.times_called/value?fun=rollup()
/api/name/linux.proc.stat/value?fun=rollup(derivative(compound(:cpu)))
/api/name/linux.proc.stat/value?fun=rollup(:merge,derivative(compound(:cpu)))
/api/name/linux.proc.stat/value?fun=rollup(:replace,derivative(compound(:cpu)))
~~~
