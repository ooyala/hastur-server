require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'uuid'
require 'socket'
require 'termite'

require "hastur"
require "hastur-server/message"
require "hastur-server/util"

module Hastur
  class Router
    attr_reader :uuid, :errors, :dynamic

    #
    # r = Hastur::Router.new('e315debb-50ba-47a6-9fb4-461757fe1e78')
    #
    def initialize(uuid, opts = {})
      raise ArgumentError.new "uuid (positional) is required" unless uuid 
      raise ArgumentError.new "uuid must be in 36-byte hex form" unless Hastur::Util.valid_uuid?(uuid)

      @uuid                   = uuid
      @route_ids              = {} # hash of id => [ {route}, {route}, ... ]
      @dynamic                = {} # hash of client_uuid => [socket, [zmq parts]]
      @timestamps             = {} # hash of client_uuid => timestamp (float)
      @handlers               = {} # hash of socket id => blocks for integrating extra sockets into the poller
      @hmac_key               = opts[:hmac_key]
      @logger                 = Termite::Logger.new
      @poller                 = ZMQ::Poller.new
      @stats                  = { :type => 0, :to => 0, :from => 0, :to_from => 0, :missed => 0 }
      opts[:stats_interval] ||= 5   # default to send stats every 5 seconds
      @errors                 = 0
      @num_msgs               = 0
      @stats_interval         = opts[:stats_interval]
      @error_socket           = opts[:error_socket]
      @last_stat_flush        = Time.now
      @last_expiry_check      = Time.now
      @dynamic_route_expire   = opts[:dynamic_route_expire] || 86400
    end

    #
    # return a useful socket identity, falling back to the memory address of the socket
    # if it no identity was assigned with setsockopt(ZMQ::IDENTITY, "").
    #
    def sockid(socket)
      if socket.kind_of? ZMQ::Socket
        rc = socket.getsockopt(ZMQ::IDENTITY, id=[])
        if ZMQ::Util.resultcode_ok?(rc) and id[0]
          id[0]
        else
          socket.socket.address
        end
      elsif socket.kind_of? FFI::Pointer
        socket.address
      else
        raise ArgumentError.new "Cannot generate a useful identity for the socket."
      end
    end

    #
    # Create a routing rule on route :to from socket :src to socket :dest.  This doesn't actually
    # run much except for parameter checking and adding the rule to the internal list.
    #
    # Options:
    #  :to      - either a symbolic route or a route UUID
    #  :src     - ZMQ socket to read from
    #  :dest    - ZMQ socket to write to
    #  :static  - (bool) this route cannot be modified at runtime
    #
    # Examples:
    # r.route :type => :stat, :src => client_router_sock, :dest => stat_sink_sock
    # r.route :type => :stat, :src => client_router_sock, :dest => stats_tap_sock
    # r.route :type => :log,  :src => client_router_sock, :dest => cass_log_sock
    # r.route :type => :log,  :src => client_router_sock, :dest => file_sink_sock
    # r.route(
    #   :type => :log,
    #   :from => '62780b2f-8d12-4840-9c6e-e89dae8cd322',
    #   :src  => client_router_sock,
    #   :dest => console_debug_sock,
    # )
    # r.route(
    #   :from => '93218295-6081-4871-b9df-6c3961a9ae94',
    #   :to   => 'bc7dbea3-da62-477c-88bd-468481a68d6b',
    #   :src  => client_router_sock,
    #   :dest => event_ack_tap_sock,
    # )
    #
    def route(opts)
      unless opts[:to] or opts[:from] or opts[:type]
        raise ArgumentError.new "One or more of :to, :from, or :type is required"
      end

      raise ArgumentError.new ":src is required"  unless opts.has_key? :src
      raise ArgumentError.new ":dest is required" unless opts.has_key? :dest

      unless opts[:src].kind_of? ZMQ::Socket
        raise ArgumentError.new "Only ZMQ::Sockets are allowed and :src is a #{opts[:src].class}."
      end

      unless opts[:dest].kind_of? ZMQ::Socket
        raise ArgumentError.new "Only ZMQ::Sockets are allowed and :dest is a #{opts[:dest].class}."
      end

      # ZMQ::Poller already checks if it already knows about a socket, so let it handle duplicates
      # only poll for readability in the primary poll, only poll writability once and let it block as needed
      @poller.register_readable opts[:src]

      route = {
        :src     => opts[:src],
        :dest    => opts[:dest],
        :static  => opts[:static] ? true : false
      }

      if opts[:to]
        if Hastur::Util.valid_uuid?(opts[:to])
          route[:to] = opts[:to]
        else
          raise ArgumentError.new ":to must be a valid Hastur route (id or symbol) or a 36-byte hex UUID (#{opts[:to]})"
        end
      end

      if opts[:from] and Hastur::Util.valid_uuid?(opts[:from])
        route[:from] = opts[:from]
      end

      if opts[:type]
        if Hastur::Message.type_id? opts[:type]
          route[:type] = opts[:type]
        elsif Hastur::Message.symbol? opts[:type]
          route[:type] = Hastur::Message.symbol_to_type_id(opts[:type])
        else
          raise ArgumentError.new ":type must be a valid Hastur::Message type"
        end
      end

      src_id = sockid(route[:src])
      @route_ids[src_id] ||= []
      @route_ids[src_id] << route
    end

    #
    # return all of the routes, mostly for dumping to console at the moment
    #
    def routes
      { :static => @route_ids, :dynamic => @dynamic }
    end

    #
    # Allow registration of a single block for a socket to be checked in the poll() loop.
    # This is for things like control sockets to manage a router.
    #
    def handle(socket, &block)
      @poller.register_readable socket
      @handlers[sockid(socket)] = block
    end

    def forward(socket, *list)
      out = list.flatten.map do |part|
        # messages are not reusable: copy each message (usually 1) from the cache
        if part.kind_of? ZMQ::Message
          new = ZMQ::Message.new
          new.copy part.pointer
          new
        else
          ZMQ::Message.new(part)
        end
      end

      rc = socket.sendmsgs out
      out.each { |part| part.close }
      rc
    end

    #
    # poll all of the sockets set up via .route() for read and route messages based on those rules
    #
    def poll_zmq(zmq_poll_timeout=0.1)
      rc = @poller.poll(zmq_poll_timeout)

      # nothing waiting or socket error, take a hit and make sure we don't spin a CPU
      if rc < 1
        @errors += 1
        sleep zmq_poll_timeout
        return rc
      end

      @poller.readables.each do |socket|
        # use the "id" to map back to a route list
        id = sockid(socket)

        # additional socket handlers can be registered for things like control or route advertisement
        if @handlers[id]
          @handlers[id].call(socket)
          next
        end

        # everything else is expected to be a Hastur message
        rc = socket.recvmsgs zmq_messages=[]
        hastur_message = zmq_messages.pop
        hastur_envelope = zmq_messages.pop
        envelope = Hastur::Envelope.parse(hastur_envelope.copy_out_string)
        hastur_envelope.close

        # convenience variables
        from = envelope.from
        to   = envelope.to
        type = envelope.type_id

        # append this router's identity to the message envelope
        envelope.add_router @uuid

        # Write the zmq headers into the dynamic route cache on every message from a ZMQ::ROUTER socket.
        # This cache is used to route message from a pull/sub socket to the correct client on the
        # router socket.
        # The dynamic route cache is only useful on ROUTER (and maybe DEALER?) sockets.
        socket.getsockopt(ZMQ::TYPE, socktype=[])
        if socktype.member? ZMQ::ROUTER
          # cache a copy of the binary string rather than the ZMQ::Message to avoid free() headaches
          headers = zmq_messages.map { |m| m.copy_out_string }
          @dynamic[from] = [socket, headers]
          @timestamps[from] = Time.now
        end

        # keep track of how many times a message is routed so we can easily tell when a message is unroutable
        times_routed = 0
        @route_ids[id].each do |r|
          # test in the order of popularity
          # simple :type routes without :to or :from should be well over 90% of cases, e.g.
          if r[:type] == type and not r.has_key? :to and not r.has_key? :from
            forward r[:dest], envelope.pack, hastur_message
            @stats[:type] += 1
            times_routed += 1

          # r.route :to => :stat, :src => client_router_sock, :dest => stat_sink_sock
          elsif r[:to] == to and not r.has_key? :from and not r.has_key? :type
            forward r[:dest], envelope.pack, hastur_message
            @stats[:to] += 1
            times_routed += 1

          # only match on from, generally expected to be used for client debugging/test replaying, e.g.
          # r.route :from => client_uuid, :src => client_router_sock, :dest => client_tap_sock
          elsif r[:from] == from and not r.has_key? :to and not r.has_key? :type
            forward r[:dest], envelope.pack, hastur_message
            @stats[:from] += 1
            times_routed += 1

          # all three of to/from/type are specified
          elsif r[:to] == to and r[:from] == from and r[:type] == type
            forward r[:dest], envelope.pack, hastur_message
            @stats[:to_from_type] += 1
            times_routed += 1

          # very specific :to and :from exact specification
          # mostly useful for tapping a specific stream from a specific source, e.g.
          # r.route :to => :stat, :from => client_uuid, :src => client_router_sock, :dest => stat_tap_sock
          elsif r[:to] == to and r[:from] == from
            forward r[:dest], envelope.pack, hastur_message
            @stats[:to_from] += 1
            times_routed += 1

          else
            # might be interesting if the router seems slow - a high number of misses for a given route
            # isn't bad, but might be indicitave that it's time to optimize the order above
            @stats[:missed] += 1
          end
        end

        # Messages destined to clients on the ROUTER socket can only be reached via their random identity.
        # For every message we receive from a client, we cache its identity in @dynamic as a binary string
        # that can be converted back to the ZMQ envelope part before sending on the router socket. This will
        # allow ZeroMQ to route the message to the right client.
        # This is a lot like ARP on IPv4 ethernet networks.
        # Future: We may eventually want a router broadcast channel for an arp-like "who has" pattern.
        if @dynamic.has_key? to
          forward @dynamic[to][0], @dynamic[to][1], envelope.pack, hastur_message
          times_routed += 1
        end

        # no route match, should not happen really except in integration tests that don't wire everything up
        if times_routed < 1
          body_str = hastur_message.copy_out_string
          @logger.warn "unroutable message | #{envelope.to_json} | #{body_str} |"

          # forward to the error socket if it's set up
          if @error_socket.kind_of? ZMQ::Socket
            error = Hastur::Message::Error.new(:from  => @uuid, :data => {
              :error => :unroutable,
              :data  => {
                :envelope => envelope.to_hash,
                :message => body_str
              }
            })
            error.send @error_socket
          end
        end

        hastur_message.close if hastur_message

        # update stats
        @num_msgs += 1
      end

      # Expire really old dynamic routes we haven't heard from for more than a day.
      now = Time.now
      if now - @last_expiry_check > 300 # every 5 minutes
        @timestamps.each do |key,timestamp|
          if now - timestamp > @dynamic_route_expire
            @dynamic.delete(key) if @dynamic.has_key?(key)
            @timestamps.delete key
          end
        end
        @last_expiry_check = now
      end
    end

    #
    # Flushes the Router's stats to Hastur and resets the counters once time expires.
    #
    def poll_stats
      curr_time = Time.now
      # After some timeout, send the incremental difference to Hastur
      if curr_time - @last_stat_flush > @stats_interval
        Hastur.counter("router.num_msgs", @num_msgs, curr_time)
        # reset stats
        @num_msgs = 0
        @last_stat_flush = curr_time
      end
    end

    #
    # Set the shutdown flag so the run() loop exits cleanly.
    #
    def shutdown
      @running = false
    end

    #
    # run in a loop while .running == true
    #
    def run(zmq_poll_timeout=0.1)
      @running = true
      while @running == true
        poll_zmq(zmq_poll_timeout)
        poll_stats
      end
    end
  end
end
