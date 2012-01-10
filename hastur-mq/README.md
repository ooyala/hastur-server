"... after stumbling queerly upon the hellish and forbidden book of
horrors the two learn, among other hideous things which no sane mortal
should know, that this talisman is indeed the nameless Yellow Sign
handed down from the accursed cult of Hastur -- from primordial
Carcosa, whereof the volume treats..."  - H.P. Lovecraft

Hastur is a monitoring system, based on a central message bus to route
messages between components.  Hastur-mq abstracts that message bus.

It has Topics, Queues and direct messages as abstractions:
http://www.devco.net/archives/2011/12/11/common-messaging-patterns-using-stomp.php

Currently you should use it with Ruby 1.9 because it is based on the
OnStomp gem.  There doesn't seem to be a good, portable asynchronous
Ruby stomp client that works with JQuery *and* Ruby 1.8 *and* Ruby
1.9.  So use Ruby 1.9.

## Topics and Queues

Hastur-mq supports Topics for broadcast messages, a.k.a. fanout
messages, a.k.a. unacknowledged messages.  It also supports Queues for
messages that require acknowledgements, and which should each be
processed only by a single consumer.

## Direct Messages

Every Hastur client can have a Universally Unique ID (UUID).  This is
a string which should only be used by a single Hastur client, ever,
under any circumstances.  Direct point-to-point messages may be sent
from client to client if the sending client has the UUID for the
receiving client.

## Sending

Every Hastur Topic or Queue has a name.  You can send a hash to that
Topic or Queue, assuming you have its name.  For instance:

  HasturMq::Topic.send("/topics/GeneralChat", { "thought" => "I like ice cream" })

The hash will be serialized to JSON, additional Hastur tracking
information may be added, and then it will be sent to the Topic or
Queue.

A Direct message may be sent if you know another client's UUID:

  HasturMq::Direct.send("BobAndOnlyBobNoReallyIMeanIt", { "you" => "owe me" })

## Subscriptions

Hastur-mq has two primary abstractions for receiving messages.  One is a
direct, asynchronous subscription a la OnStomp:

  Hastur::Queue.receive_async("/queues/myincoming.queue") do |message|
    puts "Received #{message["text"]}!"
  end

This is "asynchronous" in the sense that your code will continue
onward and not stop here to receive messages.  If that was the end of
your Ruby script, it will fall off the end and immediately exit.
Would you prefer a synchronous call?  You may do it manually:

  t = Thread.new do
    Hastur::Queue.receive_async("/queues/myincoming.queue") do |message|
      puts "Keep going!"
      exit if message["quit?"] == "true"
    end
  end
  t.join

The other abstraction is to use general delivery (see below), which is
also asynchronous.

## General Delivery

To receive all your messages in one place, don't subscribe using
HasturMq::Topic or HasturMq::Queue, but directly to HasturMq.  This
allows you to receive Topics and Queues together, multiple Topics or
multiple Queues together, and allows you to receive direct messages
sent to your UUID, if you've set one.

If you don't have a General Delivery handler, you can't receive direct
messages.

  HasturMq.receive_async do |message|
    puts "I just got #{message["text"]}."
  end
