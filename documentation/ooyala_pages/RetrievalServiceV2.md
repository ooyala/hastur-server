THIS SERVICE IS NOT DEPLOYED TO PRODUCTION YET!

Some of this information, especially on parameters, is highly applicable to API v1.

Use it but don't trust it all for V1 -- some of it is V2-ONLY!

THIS SERVICE IS NOT DEPLOYED TO PRODUCTION YET!

--------------------------------------

# Retrieval

You can test this service via curl:

~~~
# example 1
curl "http://hastur.ooyala.com/v2/statusz
# example 2
curl "http://hastur.ooyala.com/v2/query?uuid=`cat /etc/uuid`&type=gauge&name=my-server.rack&ago=one_week"
~~~

The API is [documented on yard-doc.ooyala](http://yard-doc.ooyala.com/docs/hastur-server/Hastur/Service/RetrievalV2).

## Retrieval query params

The query API is the most important part of most uses of the service.

You'll simply query [http://hastur.ooyala.com/v2/query](http://yard-doc.ooyala.com/docs/hastur-server/Hastur/Service/RetrievalV2#%2Fv2%2Fquery-instance_method), with a variety of useful and interesting parameters.  Here are those parameters:

* _kind_ - Usually "message" for full JSON messages or "value" for just values.  Can also be "csv", "count" or "rollup".  Default: value
* _start_ - Starting timestamp in microseconds since the epoch.  You can get one via Hastur.timestamp().  Usually defaults to 5 minutes ago, but can default to 1 day ago for certain messages.
* _end_ - Ending timestamp in microseconds since the epoch.  You can get one via Hastur.timestamp().  Defaults to right now.
* _ago_ - Instead of start and end, this specifies that the start was a certain length of time ago.  Usually one of a set of values like five_minutes, one_hour, two_days and so on.  Specify a non-time value like "avocado" and the server will send you a full list.
* _rollup_period_ - Also return rollups with the given period.  If "kind" is value or message, return rollups also.  If "kind" is rollup, return only rollups.  Specify a non-time value like "avocado" and the server will send you a full list.
* _uuid_ - Host UUID(s) to query for.
* _type_ - Message type(s) to query for, such as "gauge" or "heartbeat".
* _name_ - Message name(s) to query for - supports wildcards via the "*" character.
* _app_ - Application name(s) to query for - supports wildcards via the "*" character.  Deprecated in favor of label query.
* _limit_ - Maximum number of values to return.  Advisory, not always followed.
* _reversed_ - Query earliest-first instead of latest-first.  Used with _limit_.
* _consistency_ - Cassandra consistency to read at, such as 1, 2, any or quorum.
* _raw_ - Don't merge messages into the return data, but return it as escaped JSON inside the returned JSON.  For debugging.
* _label_ - Filter on labels using a "label1=blue:label2:label3=app:!label4" format (URL-encoded).  See below.
* _cb_ - For JSONP output, give the callback name as this parameter.
* _profiler_ - Return an additional top-level UUID called "profiler" with profiling information about this individual request.

Parameters can be a single value or (in some cases) a comma-separated list of values.

## Performance

Your queries will be the fastest if they supply a list of UUIDs to query and what message type(s) to query for those UUIDs.  If no such list is given but a type, app name or message name is given then Hastur will do lookup queries to determine more specifically what types and UUIDs have been written in the given day(s).  So: specifying explicit UUIDs and message type(s) gives the fastest performance.

Querying labels is very slow - the server must get all of the full message bodies, deserialize them, and check your label query string.  Labels can be useful, but try to narrow your results down first so that no more messages need querying than absolutely required.  There will eventually be label indices as well, but that's not happening soon.

Querying values is faster than querying messages and sends far less data.  If you also check the labels, that speed advantage goes away.

Querying rollups (see below) is generally much, much faster than querying all the raw messages, and is always more predictable in total size/speed.

## Rollups

There are multiple ways to query rollups.  One is to query with a "kind=rollup" query parameter.  You should also specify the rollup_period.  This will return only rollups.

You can also specify rollup_period but give "kind=value" or "kind=message".  This will return rollups as well as the requested data [*].

Or you can specify "fun=rollup()" which will start from un-rolled samples and dynamically roll them up for you.  See [Hastur Retrieval Functions](Retrieval Functions).  Certain other functions will do rollups for you as well like bin() or segment().

[*] We may deprecate this soon in favor of "just return rollups" or "just return data".  It's a weird halfway data type that can be hard to process, especially with retrieval functions.

## Retrieval functions

See [Hastur Retrieval Functions](Retrieval Functions).

## Label Queries

A label query allows searching for messages based on what labels that message has.  In version 1, label queries were very, very expensive, requiring deserializing the messages on the server and sorting them in Ruby.  In version 2, we have a complex multilevel index for labels which makes the queries less expensive, though still not cheap.  In both cases, try to also restrict your search by message name, message type, UUID and other fields to reduce the load of label queries.  And a query that returns a huge amount of data is always expensive, regardless of how you specify it.

With that said, a label query can be a really excellent way to pick a needle out of a haystack, such as querying a particular process ID's metrics or a single request ID with a known value.

Syntactically, a label query looks like this:

    curl "http://hastur.ooyala.com/v2/query?label=pid:37149,app:bozon,!phased,url=http://apiv7/*/msg&type=gauge"

This specifies that we want gauges with the label "pid" having value "37149", the "app" label is "bozon", the "url" label starts with "http://apiv7" and ends with "/msg" and there is no "phased" label at all.

This is a *great* example of what label queries are good for -- a small amount of retrieved data, grabbed from attributes with far too many values to put into the message name.  See "Data Architecture" in the Hastur User Guide for more information about what should or should not go into a message name.

There are a couple of restrictions on label queries:  excluded labels can't have a value, just a name, so you can't exclude only "!phased:true" -- though you *can* query for "phased:" if its value is empty.

And label *names* can't use the asterisk for wildcard queries, only label values (and message names).

## Constants

You can find constants related to the API [on yard-doc.ooyala](http://yard-doc.ooyala.com/docs/hastur-server/Hastur/API/Constants).

This includes types and subtypes, return value formats, which types default to returning a full day of data, rollup periods and more.

# Lookup

~~~
# Get a top-level directory of services
curl "http://hastur.ooyala.com/v2/
# Get a directory of lookup services
curl "http://hastur.ooyala.com/v2/lookup
~~~

Lookup routes need restructuring.  Final setup TBA.

# Post Messages

TBA
