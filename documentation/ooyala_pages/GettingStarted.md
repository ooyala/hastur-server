## Installing the Agent

Hastur uses an "agent" -- a process that runs locally on your machine and forwards messages from your Hastur-enabled applications to the Hastur back-end servers.

You'll need it to be installed and running.

### Linux

Type "ps aux | grep -i hastur | grep -i agent".  If it produces no output, the agent isn't running.

If the agent is running, you'll see output roughly like this:

~~~
60442    538962  0.1  0.1 190960 29040 ?        Sl   03:45   0:51 /opt/hastur/bin/ruby /opt/hastur/bin/hastur-agent.rb --uuid 0eb214f0-8a95-11e1-aff2-1231391b2c02 --router tcp://127.0.0.1:8126
~~~

To install the agent on Ubuntu, type this:

~~~
curl http://apt.us-east-1.ooyala.com/hastur.sh |sudo bash -s
~~~

(Note: you may need to change us-east-1 to another region for servers in other regions.)

### Mac OS

Type "ps aux | grep -i hastur | grep -i agent".  If it produces no output, the agent isn't running.

If the agent is running, you'll see output roughly like this:

~~~
60442    538962  0.1  0.1 190960 29040 ?        Sl   03:45   0:51 /opt/hastur/bin/ruby /opt/hastur/bin/hastur-agent.rb --uuid 0eb214f0-8a95-11e1-aff2-1231391b2c02 --router tcp://127.0.0.1:8126
~~~

Right now, there isn't a simple packaged way to run the agent on Mac OS X.  But you can check out the hastur-server repo from git and run the agent like this:

~~~
git clone ssh://git@git.corp.ooyala.com/hastur-server
bundle package --all  # Don't need --all in Bundler 2.0+
uuidgen > ~/.hastur/uuid
bundle exec bin/hastur-agent.rb --uuid `cat ~/.hastur/uuid` --router tcp://hastur-core1.us-east-1.ooyala.com:8126 --router tcp://hastur-core2.us-east-1.ooyala.com:8126 --router tcp://hastur-core3.us-east-1.ooyala.com:8126

# Or: bundle exec bluepill --no-privileged load bin/bluepill-hastur-agent.pill
# If you do this, make sure you can write to /var/run/bluepill and that it exists.
# Also, Bluepill doesn't work under JRuby.
~~~

## Testing

Your agent will automatically send back information as soon as you start it, so there should be some data from you in the Hastur servers if all is well.

Let's see if all is well.

You'll need to find your UUID and then query it.

### Finding your UUID

Your machine has a unique identifier that it uses to send data to Hastur.  If you're running the agent as yourself (Mac OS) then that UUID will probably be found in the file ~/.hastur/uuid, or possibly ~role-hastur/.hastur/uuid.

If those files aren't present, the Hastur agent will use the file /etc/uuid.

### Query with Your UUID

To query information sent from your UUID, do this:

`curl "http://hastur.ooyala.com/api/node/47e88150-0102-0130-e57d-64ce8f3a9dc2?ago=one_day"`

Replace the big chunk of random letters and numbers with your own UUID, which should look similar.

You'll either get a giant block of text with different messages types, or a message like this:

~~~
{"status":"None of 47e88150-0102-0130-e57d-64ce8f3a9dc2 have sent any messages recently.","message":404,"backtrace":[],"url":"http://hastur.ooyala.com/api/node/47e88150-0102-0130-e57d-64ce8f3a9dc2?ago=one_day"}
~~~

If it says "None of (UUID) have sent any messages recently", that means your messages aren't getting through to the server -- make sure your agent is running, or ask us for help!  Occasionally you may need to restart the Hastur agent process (just kill it, the process monitor will restart it).  It's a bug and we're working on it.

Otherwise, you're good to go.

## Logging Data

Next, you'll want to get some real data into Hastur.

First, install the Hastur gem: `gem install hastur`

### From the command line

The Hastur gem installs a command-line utility, "hastur", that sends Hastur messages.  You can do basic testing like this:

~~~
$ hastur counter analytics-athena.requests 4 --labels app=athena type=fake
$ hastur heartbeat cnd-slow-cron-job.cmdline.heartbeat
~~~

Use your own team or application name, of course.

### From Ruby

You can easily write or modify a Ruby app to send Hastur stats.  I'm just going to point you at [the Hastur gem documentation](http://yard-doc.ooyala.com/docs/hastur/frames).

There is also a [Hastur C client](http://yard-doc.ooyala.com/doxygen_hastur/hastur_8h.html), a [Hastur Go client](http://yard-doc.ooyala.com/godoc_hastur-go/pkg/index.html) and a Hastur Scala client in the hastur-c, hastur-go and hastur-scala repos.

### With a Rack Server

If you're using Rails, Sinatra, Sinatra-Synchrony or most other Ruby web servers, there's a really easy way to get some data into Hastur -- [Hastur/Rack](Rack).

Hastur::Rack is perfect if you're building a simple Rack server and want a quick dashboard to show you the status.  It focuses on what you can tell from the Rack signature -- requests that perform badly, latencies, numbers of requests and numbers of active servers.

## Dashboards

If you're using Hastur::Rack, it's really easy to see your data.  Point your browser at "http://hastur.ooyala.com/overwatch", click on Hastur::Rack in the left navigation bar, and then select your server's prefix from the upper-right drop-down list.  Bookmark it -- that's your dashboard.

Otherwise, it's possible to build a custom dashboard, but that's a more advanced topic.  You can find sample dashboard code by starting at [http://hastur.ooyala.com]() and looking at the ones in the Dashboards menu -- Hastur dashboards are usually all-JavaScript, which allows you to just view their complete source.  They usually fetch directly from the retrieval service and render the data in your browser.

For more information, see the [Hastur User Guide]().

## Retrieving Data

You can also query raw data through the Hastur Retrieval Service, a simple JSON REST server.

That's a more advanced topic, but the YARD [Retrieval API documentation](http://yard-doc.ooyala.com/docs/hastur-server/Hastur/Service/Retrieval) can help you get started.  Later, see the [Hastur/User Guide](User Guide) section on retrieval.

In general you'll test with curl on the command line to get your query right.  Then you can automate it and/or put it into a dashboard.

# Where Next?

See the [Hastur/User Guide](User Guide) for more detail.
