# Hastur FAQ

## Why is your JSON data so deeply nested?

Hastur's default JSON format for time series data is a 3-level deep nested associative array (Hash). It looks
something like { :uuid => { :metric_name => { :timestamp => :value } } }. This format allows the query server
to return any number of combinations of uuid/metric/values in a single request consistently across all of our
data. It's a little clunky to work with in javascript, but it's consistent no matter what you're fetching which
we felt was better than having a lot of special cases.

### Why are the timestamps in the JSON represented as strings?

JSON does not allow integers as keys on Objects. REST V2 API will probably address this by using arrays for the
series.

## Why another time-series database?

We looked at the available options in open source and after evaluating our requirements for high availability,
essentially arbitrary tagging, and multi-node scalability, we deceded to write our own on top of Cassandra, which
turns out to be really well suited to the task.  Ooyala runs a lot of Cassandra, which made it an obvious choice.

### What about Graphite?

Graphite is primarily a server-side graph rendering system with a lot of cool aggregation functions and a very nice
renderer. While lots of people are using it purely as a storage system, it still rolls up data in Whisper where we
want to use modern distributed storage to enable keeping full-resolution data online for as long as possible.

### What about OpenTSDB?

We didn't care for the API and at the time when we started, there was no standard way to make Hive highly available. Our
team had a lot of experience with Cassandra so off we went. Gnuplot is also not interesting at all in this day and age of
new Canvas/WebGL/SVG graphing libraries appearing daily.

### What about rrdtool?

RRDtool is a solid piece of technology that has served well over its long lifetime. It's incredibly hard on IO subsystems
with large numbers of stats and rolls up data we want to keep (hence, round-robin database).

## Does this replace Nagios?

Not really. A system with sufficient metric coverage can leave behind most Nagios checks.

## Why can't I query by hostname?

Hostnames are often wildly inaccurate even in meticulously managed networks. They get reused, mispelled, changed by accident,
and sometimes are totally meaningless (as in EC2). Hastur agent, by default, creates /etc/uuid if it doesn't exist and writes
a UUID in there that it uses from then on to identify the system. As many of the network names as are available are read
from the system and stored in Hastur's lookup_by_key column family for later translation back to hostnames.  Ohai data is
also used in this stage where available.

## What about security?

The only authentication method currently available is HMAC-SHA256 message authentication. This should be sufficient for
most uses but has the weakness that if the HMAC key is compromised, all keys are compromised.  It uses Ruby's OpenSSL
bindings so it should be fast enough for general use.

## Are counters using Cassandra counters?

No. Counters as reported by Hastur.counter are stored in the "Counters" column families. This has nothing
to do with Cassandra counters that are not currently used by Hastur.

