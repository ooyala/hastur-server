#!/usr/bin/env ruby

# hastur-sentinel - a fairly dirty full-loop monitoring daemon for hastur production
#
# This should catch most errors where we're losing messages due to thrift_client
# breakage, broken hastur-core, or any connectivity issues between all the systems.
#
# Starts up an agent for each router specified on the command line and sends messages
# through that agent, then hits the retrieval service to verify that the message was
# written to the database and is readable.
#
# This is also handy with really low wait/stall values to test various message loss
# scenarios. I already used it to show that the agent HWM is working as expected and
# resending messages when the router comes back up. This is why it prints the curl
# command on failures.
#
# TODO: rewrite it. Globals, nasty hackery on alerts, needs retries, etc.
# Good Parts:
#  * the approach seems to work
#  * pagerduty integration
#  * can run in really tight loops and saves metrics to Hastur

require "logger"
require "yajl"
require "multi_json"
require "trollop"
require "httparty"
require "tempfile"
require "date"

require "hastur/api"
require "hastur-server/time_util"
require "hastur-server/util"
require "hastur-server/service/agent"

include Hastur::TimeUtil

NAME_PREFIX = "hastur.sentinel"
$INCIDENTS = {}

opts = Trollop::options do
  banner <<-EOS
hastur-sentinel.rb - monitor hastur end-to-end

Connects to each Hastur router using a different socket, sends unique stats to each, then
waits a small amount of time before checking the retrieval service for the stats.

  Options:
EOS
  opt :router,    "Hastur Router URI",                :default => ["tcp://127.0.0.1:8126"], :multi => true
  opt :retrieval, "Hastur Retrieval Service URI",     :default => "http://127.0.0.1:9393"
  opt :uuid,      "System UUID",                      :required => true, :type => String
  opt :wait,      "Seconds to wait between checks",   :default => 30.0
  opt :timeout,   "Retrieval service timeout",        :default => 2.0
  opt :stall,     "Seconds to wait between send/get", :default => 0.5
  opt :pagerduty, "pagerduty API key",                :required => true, :type => String
  opt :debug,     "Enable debug logging",             :default => false
end

logger = Logger.new(STDERR)
if opts[:debug]
  logger.level = Logger::DEBUG
else
  logger.level = Logger::WARN
end

ctx = ZMQ::Context.new

agents = opts[:router].flatten.map do |router_uri|
  socket_file = Tempfile.new "router-#{router_uri.hash}"
  id = router_uri.gsub /\W+/, "."

  agent = Hastur::Service::Agent.new(
    :ctx     => ctx,
    :routers => [router_uri],
    :uuid    => opts[:uuid],
    :unix    => socket_file.path,
  )

  # set up sockets
  socket_file.unlink # file can't exist when the socket opens
  agent.setup

  # don't run the agent before returning, we're driving manually
  { :agent => agent, :uri => router_uri, :id => id }
end

# set up signal handlers and hope to be able to get a clean shutdown
alive = true
%w(INT TERM KILL).each do |sig|
  Signal.trap(sig) do
    agents.each { |a| a[:agent].stop rescue nil }
    alive = false
    Signal.trap(sig, "DEFAULT")
  end
end

def alert_pagerduty(api_key, incident_id, message, details)
  event = {
    :service_key  => api_key,
    :incident_key => incident_id,
    :event_type   => "trigger",
    :description  => message,
    :details      => details,
  }

  puts "Posting JSON data to pagerduty:\n#{MultiJson.dump(event, :pretty => true)}"
  reply = HTTParty.post "https://events.pagerduty.com/generic/2010-04-15/create_event.json",
                        :body => MultiJson.dump(event)
  puts "Alert posted, reply is #{reply.inspect}"

  if reply.code >= 200 and reply.code < 400
    $INCIDENTS[incident_id] = true
  else
    puts "Error creating PagerDuty incident: #{reply.inspect}"
  end
end

def failed(req, url, query, options, elapsed, message)
  full = "#{url}?#{query.map { |k,v| "#{k}=#{v}" }.join("&")}"
  longmess = "FAIL on #{options[:uri]}: #{req.code rescue '???'}, #{req.body.length rescue 0} bytes, #{elapsed} usec, #{message}"

  # one incident ID / day, keep the pager sane
  incident_id = [options[:uri].to_s[6..21], Date.today.to_s].join("/")

  unless $INCIDENTS[incident_id]
    details = options.merge(:elapsed => elapsed, :query => query, :url => url, :request => req)
    details.delete :pagerduty
    alert_pagerduty options[:pagerduty], incident_id, longmess, details
  end

  puts "#{longmess}\ncurl '#{full}'\n#{req.body rescue ''}"
end

def check(timestamp, options)
  base_url = options[:retrieval]
  uuid = options[:uuid]

  name = "#{NAME_PREFIX}.#{options[:id]}"
  url = "#{base_url}/api/node/#{uuid}/name/#{name}/message"
  query = { :type => :mark, :start => timestamp, :end => timestamp, :pretty => :false }

  req = begin
    req = HTTParty.get url, :query => query, :timeout => options[:timeout]
  rescue Exception => e
    failed req, url, query, options, usec_epoch - timestamp, "HTTP request timed out: #{e}"
    return
  end

  # subtract stall from the roundtrip time on success
  elapsed = (usec_epoch - timestamp) - (options[:stall] * USEC_ONE_SECOND)

  if req.code.to_i.between?(200, 299)
    d = MultiJson.load(req.body)

    unless d[uuid] and d[uuid][name]
      failed req, url, query, options, elapsed, "Empty JSON object in response!"
    end

    entry = d[uuid][name][timestamp.to_s] rescue nil

    if entry and entry["timestamp"].to_i == timestamp
      puts "OK (#{elapsed} usec) #{uuid}/#{name}/#{timestamp}"
      Hastur.gauge "#{NAME_PREFIX}.roundtrip.usec", elapsed, timestamp, :units  => :usec
    else
      mess = "sent timestamp not in dataset or corrupt response"
      failed req, url, query, options.merge(:entry => entry), elapsed, mess
    end
  end
end

count = 0
wait_seconds = opts[:wait].to_f / agents.count.to_f

while alive
  agents.each do |agent|
    now = usec_epoch
    agent[:agent].override_hastur_sender
    Hastur.mark "#{NAME_PREFIX}.#{agent[:id]}", agent[:uri], now, :iteration => count

    sleep opts[:stall]

    check now, opts.merge(agent)

    sleep wait_seconds
  end

  count += 1
end

agents.each { |a| a[:agent].shutdown rescue nil }
ctx.terminate

exit 0
