Overview
--------

Hastur is a monitoring system focused on removing any barriers to
entry for engineers who need their systems monitored.

The communication and most points of scalability are built on top of
ZeroMQ, allowing the agent daemon to be thin and simple.

Message storage is done with Cassandra, a highly-scalable key-value
store with excellent support for time-series data.

Hastur supports RESTful querying of data using the retrieval service
and streaming examination of data using triggers.

Components
----------

* Hastur Agent - a daemon that runs on individual monitored boxes.
  The agent is often installed via a Debian package with a
  self-contained Ruby installation to avoid system dependencies.

* Hastur Core - a server daemon that receives messages and writes them
  to Cassandra.

* Hastur Retrieval service - a REST server to retrieve batches of
  messages from Cassandra and return them as JSON.  This also
  retrieves hostname data, UUID messages sources and message names to
  allow exploration of who is sending what data.

* Hastur Syndicator - a server daemon to receive events from Core and
  send them out to workers with triggers as a realtime stream.

API Documentation
-----------------

All components of Hastur use YARD for basic documentation.  Install
YARD via "gem install yardoc redcarpet" and then type "yardoc" to
generate HTML documentation in doc/index.html.

Architecture
------------

Individual hosts are assigned a UUID identifier and run an agent
daemon.  The agent opens a UDP port (normally 8125) for local use.
The agent sends back system information always, and also forwards
Hastur messages from individual Hastur-enabled applications.

The agent forwards messages to the Core servers, which then store
them, forward them to streaming Syndicators and write to Cassandra for
later retrieval.

Using ZeroMQ and/or Cassandra, each component other than the agent can
have as many copies as desired for availability and/or fault
tolerance.  The agent isn't duplicated because, fundamentally, Hastur
cannot provide fault tolerance at the single-host level.  That must
be done by the application(s), if at all.

Development
-----------

Install ZeroMQ 2.x
Install Cassandra 1.1

Also, bundle install from the root directory. Look at the integration
tests under tests/integration.

Debugging Tips
--------------

If you're not sure if data is coming in on the UDP port, the first thing to check after logs
is tcpdump on localhost. This is generally safe to run during production, just don't leave it
running for a long time.

    sudo tcpdump -ni lo -vX -s 65535 port 8125

On OSX

    sudo tcpdump -ni lo0 -vX -s 8192 port 8125

Once you've verified that data is getting to the agent on UDP, you can snoop the outbound ZeroMQ
port to see if the same data is making it through the agent.

    sudo tcpdump -ni eth0 -vX -s 1500 port 8126

Dependencies
------------

* Ruby 1.9.3
* ZeroMQ 2.x (2.2.11) - some changes required for 3.x
* Gems in Gemfile

Deployment
----------

The agent is deployed via Debian packages (other methods later)
Core is deployed via debian packages
Triggers - automated deployment pending

README for Triggers
-------------------

## Usage

Files that use the alerting API are called triggers, and will be
pushed to a special Git repo for that purpose and marked as runnable
in production after passing a set of tests.  It's possible to run the
same tests locally on your own machine, of course.

Here's an example of a Hastur trigger:

  # variable_load_trigger.rb
  ctx = Hastur::Trigger::Context.new

  ctx.gauges(:name => "ots.transcoding.load", :labels => { "send_to" => "load_tester" }) do |msg|
    if msg.value > 10.0
      # PagerDuty requires an incident ID, a message, and has an optional
      # JSON hash of extra stuff.  Pass in the message automatically?  Or
      # just its UUID and timestamp?
      pager_duty("Monitoring-load-spiking-#{msg.uuid}",
                 "The load has spiked to #{msg.value} on host #{msg.hostname}",
                 :message => msg.to_json, :load => msg.value, :uuid => msg.uuid,
                 :hostname => msg.hostname)

      ctx["total"] ||= 0
      ctx["total"] += 1
    end
  end

  ctx.every(:minute) do
    Hastur.gauge("ots.transcoding.load.spikes", ctx["total"])
  end

The Trigger context object allows you to subscribe to Hastur messages
using a code block to process the messages, and also lets you store a
hash of limited size which must be fully serializable to JSON.

Hastur will run a set of syndication servers and supervisor processes
which will filter the Hastur "firehose" of events to all the various
triggers that want to see them.  That's why the triggers subscribe to
particular event types with additional filtering.  Initially the
filtering will be very simple with the full firehose going to each
supervisor and the supervisor filtering events down to individual
triggers, but eventually we hope to be much smarter about who sees
what.

The block of ruby code for each event type runs in the same context,
and all blocks in the same file share a singe serializable hash
object.  That's useful if you want to handle both statistics and
events in such a way that an alert can be raised manually or
automatically but you won't see both if both are raised (you can also
de-dup with PagerDuty event names).  It's also useful for correlating
multiple message sources or multiple message types to find out about a
single problem.

The hash object is primarily for the purpose of correlating events
over time - often you may want to do filtering like "are more than 10%
of API calls errors?" or "am I seeing at least 6 requests out of each
100 with latency over a second?"  This can be achieved by putting
counters or (small) event buffers into the hash object.

The hash will have a number of restrictions, enforced by the tests
mentioned above.  It must work fine if the hash is serialized to JSON
and restored in between every request, or never serialized, or
serialized only sometimes -- this is to reflect that we may need to
migrate a trigger between supervisor hosts in between requests, or
restart a flow of messages from saved state.  The hash must also be
the *only* saved state - things like instance variables will not be
saved and the test will reject triggers that are caught setting
instance or class variables.  We can't easily save and restore them,
so they will result in inconsistent trigger behavior.

We will also enforce that the same messages replayed in the same order
will give the same state and notifications.

We would like to enforce, but probably won't, that triggers should be
as order-independent as possible for processed messages since Hastur
allows out-of-order message delivery by its nature.  However,
enforcing order-independent triggers is almost certainly not
practical.

### API

The message subscriptions will include at least the basic Hastur
message types like gauges, heartbeats, process registrations and so
on.  It will also be possible to filter messages by name, value, attn,
subject, labels and uuid of the sending agent.  Eventually it will be
possible to filter UUID by name groups -- that is, by tags set when
registering the host itself with Hastur, which will be easier to
update than a manual list of UUIDs.  For the initial deploy this may
need to be done with labels on the messages themselves.

There will also be an "every" special subscription which is called
with roughly the given interval - minute, hour, day, etc.  The "every"
callback is only guaranteed to be called during an interval when at
least one message is received by the trigger, so a trigger that
processes only host registrations and has an every(:minute) call may
not receive the "every minute" call nearly that often.

Essentially, "every" is syntactic sugar for keeping a "last sent" time
and doing something every time any message arrives if it has been
longer than that time.  It is marginally more efficient than that
approach and significantly prettier, but does not fully replace it.

### UUIDs

Triggers may create new Hastur messages.  This is the mechanism by
which derived statistics can be created.  For instance, by subscribing
to load statistics across a large number of UUIDs and calling
Hastur.gauge() to create an average statistic, it is straightforward
to keep a (derived) statistic with the average load across these
systems over time, possibly at a different rate than they originally
sent back (example: sample average load once/minute with an
every(:minute) callback).  For now, this will be the obvious way to
create a dashboard that samples information across a large number of
hosts with low latency.

### State Structures

Hastur will keep track of the current hash for each trigger.  Right
now, the way to "query" these structures is to send derived statistics
or other messages from them.  Later we hope to have a REST server
which will simply allow you to query a recent state for any given
trigger as a JSON structure.

### Replaying

Occasionally we will have outages in Hastur stats, in alerting or in
syndication and replication.  When that happens, the standard recover
method will be to start from the last known-good trigger states and
replay the messages against them.  For that reason, it is important to
understand that Time.now and the message timestamps may occasionally
give *very* different results.  As a general rule, it is best to use
the message timestamps where you can because a replay situation may
replay many, many, many errors during a very short interval of
wall-clock time even when there is no actual problem occurring.

The Hastur trigger tests will do a little to try to expose this
problem, but their ability to do so is quite limited.  By their
nature, they cannot know what a "reasonable" error rate is.

### Notification

As shown in the example, triggers will have an API to create PagerDuty
alerts, send emails and otherwise notify based on the contents of a
given message.  These APIs will be fully deactivatable, both for
testing and for replay cases like the one above.  In the case of
replay it is quite likely that we will need some kind of "pending"
status for notifications when we know to expect faulty notifications
but we may also receive real, valid ones.  For v1, these notifications
will be recorded but not marked pending, allowing them to be examined
but also requiring more human intervention during an alerting outage
since the recorded notifications will not be automatically re-examined
and resent.

The PagerDuty API chosen *will* allow the notification to be raised
immediately if the problem persists after monitoring is re-enabled.
