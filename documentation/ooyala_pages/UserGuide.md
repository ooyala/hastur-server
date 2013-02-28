# Getting Started as a Hastur User

First, read [Hastur/Getting Started](Getting Started) and do what it says.

Now you have the [Hastur client](http://yard-doc.ooyala.com/docs/hastur/frames) and agent installed and you are capable of sending data.

## Hastur::Rack

The Getting Started guide will also tell you about [Hastur/Rack](Rack) (aka Hastur::Rack).  If you're building a Ruby web server, Hastur::Rack is a minimum-pain way to capture basic information before you need a full-on custom dashboard.

The flip side is that you can't easily customize it (yet?).

# Hastur Environments

We are transitioning Hastur Production to greater stability, because we want to be able to do production alerting on it.  The idea is that production Hastur data goes into the Production server (hastur.ooyala.com), while staging and development data normally go into the staging servers (hastur.staging.ooyala.com).

## Hastur Staging

It's fine to mirror a fraction of production data to staging for testing purposes, and is often a good idea.  Please don't mirror *all* production traffic to staging, as staging is a much smaller Cassandra cluster.

Staging will often have newer, less-stable versions of the Hastur infrastructure itself.

As of this writing, Hastur Staging is just being set up.

## No Hastur Next-Staging

There is currently no next-staging cluster for Hastur.  Each cluster is a lot of hardware, so we have a limited number of them.

# Getting Started with Hastur Server - Setting Up Infrastructure Locally

What if you want to run Hastur and somebody else hasn't set up the infrastructure for you?

You'll need to install and run the various Hastur back-end infrastructure.

## Install Cassandra

This has been [described well elsewhere](http://wiki.apache.org/cassandra/GettingStarted).  You should start with a simple one-node Cassandra install and running it from the command line via "cassandra -f".

Here's an example of how to install Cassandra 1.1.0 single-node:

~~~
cd ~
curl http://archive.apache.org/dist/cassandra/1.1.0/apache-cassandra-1.1.0-bin.tar.gz > apache-cass.tgz
tar zxvf apache-cass.tgz
cd apache-cassandra-1.1.0
./bin/cassandra -f   # Doesn't exit, just hangs around printing logs until you hit ctrl-c
~~~

You can create a larger Cassandra cluster when you like.  Eventually you should be running with at least 5 nodes for proper quorum-write, quorum-read and failover behavior.  This guide won't tell you how.

## Get the Repo

If you haven't already, clone the hastur-server repository from git.corp.ooyala.com or from GitHub.  It contains the code for the agent, router, sink and retrieval service, all of which you'll need for a well-functioning Hastur installation.  If you're just debugging one component you may be able to get away without running all of these, but I'll leave that decision to you.

## Set up Cassandra Schema

If you're running single node, you'll need a Cassandra schema with a replication factor of 1.  It's not good to have a replication factor higher than your total number of nodes!

Sadly, you'll also have more data loss and general annoyance than a user on a big beefy cluster -- Cassandra is always happiest and most robust with 5+ nodes.  That's okay, you can debug with one and re-image it frequently, or debug using a big shared cluster.  Both have pluses and minuses.

~~~
# From the hastur-server directory

REPLICATION_FACTOR=1 ./tools/cassandra/generate_schema_cql.sh | $MY_CASSANDRA_DIR/bin/cassandra-cli -h localhost
~~~

You'll need to use your real Cassandra directory for $MY_CASSANDRA_DIR or make sure that cassandra-cli is in your path.  Also, substitute your real Cassandra host for "localhost" if you're not running it locally.

### Troubleshooting

Single-node Cassandra has an unfortunate tendency to become corrupted in such a way that the Hastur keyspace no longer exists, can't be dropped and can't be created.  When that happens, by far the most reliable way to handle it is to remove the Cassandra data directories, often found under /var/lib/cassandra.  Don't just remove the one called "hastur", shut down local cassandra and delete all of them.  This is local, test-data-only advice!  Don't do it blindly on cluster nodes!  You will lose all data if you do this.

You can also run "$MY_CASSANDRA_DIR/bin/cassandra-cli -h localhost" and type in "drop keyspace hastur;".  Sometimes it works, sometimes it doesn't.  Again, this will destroy all your data if you do it.

## Run the Sink Locally

Hastur uses Routers and Sinks to receive the data, get it to the right place and write it to Cassandra.  It can run with one router and one sink together in a single process writing directly to (local) Cassandra, with just a sink, or with one or more routers forwarding data to sinks.

Let's set you up with just a sink, which is often how Hastur works in production and is clearly the simplest.

If your single-node cassandra is on the same machine, you can run it very simply:

~~~
bundle exec ruby bin/hastur-core.rb  # Runs the sink, configured as a runnable app
~~~

You can also specify various options.  Run hastur-core.rb with --help for details.

## Run the Agent, But Sending to Localhost

The Hastur agent is normally run via Bluepill, a process manager which will restart it if it crashes or is killed.  That's great for reliability and sucks for debugging.  You also have to run it as root.  You'll probably be served better by just running it yourself.

If you don't use bluepill, make sure to run ./bin/hastur-create-uuid, which will create a ~/.hastur/uuid file for you.  You may need to pull or check out the latest Hastur branch to get this script.

To run directly:

~~~
bundle package --all  # Don't need --all in Bundler 2.0+
bundle exec bin/hastur-agent.rb --uuid `cat ~/.hastur/uuid` --router tcp://127.0.0.1:8126
~~~

To run via Bluepill:

~~~
export HASTUR_ROUTERS="tcp://127.0.0.1:9160"  # semicolon-separated list or just one
bundle exec bluepill -c --no-privileged load bin/bluepill-hastur-agent.pill
# If you do this, make sure you can write to /var/run/bluepill and that it exists.
# Also, Bluepill doesn't work under JRuby.
~~~

If you run using bluepill, you'll need to explicitly shut it down later:

~~~
bundle exec bluepill --no-privileged quit
~~~

## Run the Retrieval Service

The Retrieval Service is a simple REST server that reads from Cassandra and sends back JSON.  You can use it directly or load-balance it like any other HTTP server with software like HAProxy for greater reliability.

As of the latest branch, you'll want to build a retrieval war file and run it.

~~~
rake retrieval_war
java -jar retrieval_v2.war
~~~

You can also supply a lot of memory optimization options if you so choose:

~~~
java -jar -Xmx5g -XX:+UseParNewGC -XX:+UseAdaptiveSizePolicy -XX:MaxGCPauseMillis=100 -XX:GCTimeRatio=19 retrieval_v2.war
~~~

You can directly run the retrieval service like the war does like this:

~~~
bundle exec rackup config_v2.ru
~~~

If you're running non-local Cassandra, you'll need to supply a list of Cassandra URIs as JSON in an environment variable.  You can see an example of production use in the script tools/restart_dev_unicorn.sh.  Or you can specify directly:

~~~
CASSANDRA_URIS="[\"hastur-core1.us-east-1.ooyala.com:9160\",\"hastur-core2.us-east-1.ooyala.com:9160\"]" java -jar retrieval_v2.war
~~~

There are multiple rackup files for different API versions -- basically, check out an old version of the server to run an old API version.  You can run multiple server versions and route to them separately by URI prefix using NGinX, Apache or another reverse-proxy -- doing so is beyond the scope of this guide.

For debugging, just run one.

# Hastur Retrieval Service

The Hastur Retrieval Service is a REST server that allows you to:

* query Hastur data in various ways
* do transforms on that data
* do lookups on names, uuids, applications and so forth that have written data in a given time period
* (beta, in development) write Hastur data without running a local agent.

All Hastur data is implicitly time-series, so all Retrieval Service queries implicitly use a time range.

You can see the retrieval protocol version 1 documented [here on Yard-Doc.ooyala](http://yard-doc.ooyala.com/docs/hastur-server/Hastur/Service/Retrieval).

(Pre-production:) Retrieval service protocol v2 is documented at [Hastur/Retrieval Service v2](Retrieval Service v2).

The Hastur retrieval service allows you to specify a variety of server-side functions to subdivide, roll up or otherwise alter the returned data.  See [Hastur/Retrieval Functions](Retrieval Functions) for details.

# Hastur Dashboards

Dashboards are static JavaScript pages that pull directly from the Hastur Retrieval Service, a REST server that returns JSON data.

[Overwatch](http://hastur.ooyala.com/overwatch) is the most up-to-date dashboard for Hastur internals and Hastur::Rack apps.  Start there.

## Overwatch

Overwatch uses the [Hastur/SeriesSource](SeriesSource) JavaScript library for querying Hastur and the corresponding graphing libraries [Hastur/SeriesRickshaw](SeriesRickshaw) and [Hastur/SeriesSparkline](SeriesSparkline) to display the data.  All are based on [D3](http://d3js.org).

Overwatch contains a top-level dashboard for monitoring Hastur itself and sub-dashboards for other projects.  See the left-hand navigation menu.

## XKCD Dashboard

The most configurable dashboard for Hastur::Rack apps is the [XKCD dashboard](http://hastur.ooyala.com/xkcd_dashboard.html).  It's good to learn from if you view source.

You can edit its parameters to show your own dashboard:

* host - the host server to query for Hastur info -- this could be 127.0.0.1 to query your local Hastur retrieval service, if you're running one.
* prefix - the server prefix.  If you use "prefix=hastur.retrieval" you should see information from the Hastur retrieval service itself.
* ago - this defaults to five_minutes, but could also be two_days or one_week or one_hour if you prefer.

There are other parameters, see the bottom of the page for more details.

## Building Dashboards

The (still very early) guide to [Hastur/Building Dashboards](Building Dashboards)

# Hastur Data Architecture

Everything in Hastur is implicitly time-series.  Messages, value, indexes, rollups, hostnames...  All of it is stored in some size/kind of time bucket.  "Implicit" data like indexes, uuid lookups and hostnames are normally stored in day-sized data buckets, either per UUID or one single shared one across all of Hastur (for that day).

For that reason, things like queries are often hard to understand unless you realize that they implicitly pass time parameters, usually defaulting to "give me the last five minutes of data" or "give me the last day of data".

All label-related indices are stored hourly because of their much greater size.

Hastur stores basic, canonical data with the following structure:

    Message type (col. family) -> UUID x time-bucket (row) -> msg-name x timestamp (col) -> JSON or MsgPack'd value (val)

Different indices store using different formats.

## Message Names

Most message types have names -- gauges do, for instance, and process registrations don't.

Messages can easily be queried with a prefix, like "I want all gauges starting with 'analytics-server.development'.  For that reason, it's good to start message names with a unique prefix for your Hastur-enabled application, something like 'athena-server'.  Your team name can be good or bad -- how sure are you that the team will exist in 18 months?  After the application name, you'll probably want to put the environment, like 'staging' or 'production'.  You'll often want to set it from a variable like RACK_ENV.

All of these recommendations are to make it easy to query your data later.

Don't put a highly-variable field into a label like an account name, a provider ID, or most error codes.  Remember that we routinely have human beings browse through these names, so we try to keep them down to a reasonable number of hundreds or thousands of names, total.  So if you add 30,000 names that are the same except for a provider ID, you've swamped everybody else's use of a stat browser into complete uselessness.

As a general rule, something with 3-5 values is no problem to embed in a stat name, such as the leading digit of the HTTP status or the set of environments your app runs in.  At 5-10 values you should be seriously trying not to do it, and above 10 I will hunt you down and hurt you.

Remember to structure your names hierarchically, since prefix queries are fast.

## Values

Some statistics also have values -- gauges have numeric values, counters are usually integer, marks have string values and so on.  It's usually pretty clear to put in the value, since that effectively defines what that message type is for.

## Marks

Marks are odd in that you can put some things in either the name or a value, like with start and end marks.  Large strings should go into the value, or if there are more than 3-5 total possibilities, or may be in the future.  If there are only two or three possible values and they're short, it might be okay to embed them in the message name.

## Labels

Other information will usually go into the labels, which are single-level hashes, usually mapping strings to strings.  You can think of each label as a form of database index for Hastur -- it slows down write a bit in order to speed up reads, especially when the cardinality/selectivity of the index is good.

In earlier Hastur versions, label queries are quite slow so you effectively have to overquery and sort out what you want.

In later Hastur versions (v2 or higher), label queries are pretty fast, and you can also query label values by prefix.  That is, you can not only query for "pid=27134", you can query for "pid=27*".  Label values are normally treated as strings for these queries, even if the value is originally numeric.

Label queries are still very slow if you want to pull back a very large number of messages -- thousands or tens of thousands, say.  Millions will probably be so slow that it doesn't return in time.  Values or (in development) counts will be faster than messages.  In general, it's very fast to use label queries to pull back a "needle in a haystack" -- a small number of messages where the label is used to locate them.  It's much slower to use labels to pull back 10% of your data from a large/frequent metric.  Again, compare this to cardinality/selectivity in MySQL or other databases.  Labels do much, much better if the cardinality of the index is good for the specific query you're doing.  A "pid exists" label query is worse than no specified label at all, while a "pid=27134" label query will get you excellent results.

There are no per-label rollups currently.  We don't know what that feature should look like yet.

## What's Fast to Query?

Names and name prefixes are fast to query, especially if you actually want all the data.

In retrieval v2 or higher, labels with good selectivity are fast to query.

If you already know the UUIDs to query, or can quickly find them from a Hastur internal index, that's especially fast to query.  The same applies, to a lesser extent, with message names.

## What's Slow to Query?

It should be no surprise that querying lots more data than you need and then sorting it out afterward is slow.

In older versions of Hastur, labels are quite slow to query.  This is because Hastur queries far more than necessary, and sorts it out afterward.  It also involves a lot of serializing and deserializing.

Cross-UUID queries are often slow, especially if your query doesn't map to a built-in Hastur index.  Consider using message names that will make your query easy.  In retrieval v2 or higher, see if there's a way to turn your query into a high-selectivity label query.

## Storage

Hastur separates data into different column families by type, so gauges and marks are queried completely separately.

Within that column family, we separate rows by UUID and time bucket.  A time bucket is the five minutes, hour, or day that a given piece of data goes into -- different metrics have different sizes of time bucket.

We also have an index column family to determine what stat names were logged from what UUIDs on what days and so on.

And then there's a complex multiple-level label index, which is its own whole topic.

## Encoding

Your raw data values can often be binary.  Your Hastur "metadata" -- message names, labels, etc -- should be encodable as UTF-8 strings.  Embedded NUL characters can cause problems with querying.

Similarly, Hastur will actually accept all kinds of screwy things we don't recommend like Hash values for gauges.  But it'll screw up querying and rollups.  Specifically, Hastur can store any value that is both JSON-encodable and MsgPack-encodable.  So basically, anything you can put into your JSON message.

Finally, you can use the empty string for label names and stat names, but in some cases that, too, will screw up querying and rollups.  Normally names and label names should not be empty, though label values can be.

# Other Documentation

You've already seen [Hastur/Getting Started](Getting Started) and [Hastur/Rack](Rack) earlier in this guide.

Other documentation includes the "Using Hastur" [slide show](http://portal.sliderocket.com/BKHPY/Using-Hastur) and [slightly outdated video presentation](http://skillshare.corp.ooyala.com/) (click "Hastur Overview").
