Overview
--------

Hastur is a monitoring system focused on removing any barriers to
entry for engineers who need their systems monitored.

The communication and most points of scalability are built on top of
ZeroMQ, allowing the agent daemon to be thin and simple.

Message storage is done with Cassandra, a highly-scalable key-value
store with excellent support for time-series data.

Hastur supports RESTful querying of data using the retrieval service.

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

The agent forwards messages to the Core servers, which then write to
Cassandra for later retrieval.

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
