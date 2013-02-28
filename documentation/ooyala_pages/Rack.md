If you're using Rails, Sinatra, Sinatra-Synchrony or most other Ruby web servers, there's a really easy way to get some data into Hastur -- [Hastur::Rack](http://yard-doc.ooyala.com/docs/gems_hastur-rack/frames).

## Install Hastur::Rack

In your Gemfile:

~~~
gem "hastur-rack"
~~~

Then run "bundle install".

Then, in your config.ru:

~~~
require "hastur-rack"

# Before "run", add these lines:
environment = ENV['RAILS_ENV'] || ENV['RACK_ENV'] || "development"
use Hastur::Rack, "my-awesome-server-#{environment}"

# Then "run" as usual -- no change here.
run MySinatraAppOrWhatever
~~~

That's the basics.  You can configure a lot about what requests count as "slow enough to log" -- too much added memory?  Too many garbage-collects?  Latency over 3 seconds?

See [the latest README](http://yard-doc.ooyala.com/docs/gems_hastur-rack/file/README.md) for full details.

But just adding the lines above will do what most people need.

## Your Dashboard

Visit [http://hastur.ooyala.com/overwatch/hastur-rack.html](http://hastur.ooyala.com/overwatch/hastur-rack.html) to see the main Hastur::Rack dashboard.  If you're sending back data, select your prefix from the upper-right menu and you should see the statistics for your server in particular.

Bookmark your Hastur::Rack dashboard and you can send people to it directly and go to it immediately.  When you select your dashboard from the menu, it redirects to a URL like "http://hastur.ooyala.com/overwatch/hastur-rack.html?prefix=my-awesome-server-staging".

## What Else Do You Get?

We're working on a "request dashboard", which can trace a request through multiple Ooyala servers and see what Hastur information got logged.  However, it only works with Hastur-enabled servers, and we need to know the request ID.  Hastur::Rack provides that tracking information if it receives it.  Other servers you call receive it *if* you set Request ID headers properly.  See [rack-ooyala-headers](http://yard-doc.ooyala.com/docs/rack-ooyala-headers/frames) for more details about forwarding the headers.

## More

See [the latest README](http://yard-doc.ooyala.com/docs/gems_hastur-rack/file/README.md) for additional details.

Your prefix mostly just has to be unique.  Which ones are already used?  You can check the Hastur::Rack dashboard menu.
