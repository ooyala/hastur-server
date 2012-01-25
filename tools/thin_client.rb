#!/usr/bin/env ruby

# this is just a whack at a really thin client script
# totally untested at this point, checking in so others can make fun of me
# if we go this route, main should become 3-4 functions and the whole lot should use the __FILE__ trick to
# make it loadable for testing

require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'uuid'
require 'socket'
require_relative "../tools/zmq_utils"

MultiJson.engine = :yajl

opts = Trollop::options do
  opt :router, "Router URI",        :type => String, :required => true, :multi => true
  opt :uuid,   "System UUID",       :type => String
  opt :port,   "Local socket port", :type => String, :required => true
end

unless opts[:uuid]
  # TODO: attempt to retrieve UUID from disk 
  opts[:uuid] = UUID.new.generate
  STDERR.puts "Generated new UUID: #{opts[:uuid].inspect}"
  # TODO: save the UUID to disk
end
CLIENT_UUID = opts[:uuid]
ROUTERS = opts[:router]
LOCAL_PORT = opts[:port]
HEARTBEAT_INTERVAL = 15  # Hardcode for now
NOTIFICATION_INTERVAL = 5 # Hardcode for now

def exec_plugin(plugin_command, plugin_args=[])
  child_out_r, child_out_w = IO.pipe
  child_err_r, child_err_w = IO.pipe

  child_pid = Kernel.spawn(plugin_command, plugin_args, 
    :out => child_out_w,
    :err => child_err_w,
    :rlimit_cpu => 5,   # 5 seconds of CPU time
    :rlimit_as  => 2**5 # 32MB of memory total
  )

  child_out_w.close
  child_err_w.close

  return child_pid, child_out_w, child_err_w
end

def process_udp_message(msg)
  STDERR.puts "Received UDP message: #{msg.inspect}"

  hash = MultiJson.decode(msg) rescue nil
  unless hash
    STDERR.puts "Received invalid JSON packet: #{msg.inspect}"
    return
  end

  if hash['method'] == "notify"
    @notifications[hash['id']] = hash
  end

  hastur_send @router_socket, hash['method'] || "error", hash.merge('uuid' => CLIENT_UUID)
end

def process_msg(message)
  STDERR.puts "Cheerfully ignoring multipart to-client message: #{message.inspect}"

  ["echo", "OK"]  # Trivial-success plugin
end

def process_notification_ack(msg)
  hash = MultiJson.decode(msg) rescue nil
  unless hash
    STDERR.puts "Received invalid JSON packet: #{msg.inspect}"
    return 
  end

  notification = @notifications[hash['id']].delete
  unless notification
    hastur_send(@router_socket, "error", { :message => "Unable to ack notification with id #{hash['id']} because it does not exist."})
  end
end

def poll_plugin_pids(plugins)
  # if we really want to be paranoid about blocking, use select to check
  # the readability of the filehandles, but really they're either straight EOF
  # once the process dies, or can be read in one swoop
  plugins.each do |pid, info|
    cpid, status = Process.waitpid2(pid, Process::WNOHANG)
    unless cpid.nil?
      # process is dead, we can read all of its data safely without blocking
      plugin_stdout = info[:stdout].readlines()
      plugin_stderr = info[:stderr].readlines()

      forward_plugin_output(router,
        :pid    => cpid,
        :status => status,
        :stdout => plugin_stdout,
        :stderr => plugin_stderr
      )

      plugins.delete cpid
    end
  end
end

def set_up_local_ports
  @udp_socket = UDPSocket.new
  STDERR.puts "Binding UDP socket localhost:#{LOCAL_PORT}"
  @udp_socket.bind nil, LOCAL_PORT

  @tcp_socket = nil
end

def set_up_router
  @context = ZMQ::Context.new
  @router_socket = @context.socket(ZMQ::DEALER)
  @router_socket.setsockopt(ZMQ::IDENTITY, CLIENT_UUID)
  ROUTERS.each do |router_uri|
    @router_socket.connect(router_uri)
  end
end

def set_up_poller
  @poller = ZMQ::Poller.new

  if @router_socket
    @poller.register_readable @router_socket
    @poller.register_writable @router_socket
  end

  @last_heartbeat = Time.now
  @last_notification_check = Time.now
end

def poll_zmq(plugins)
  @poller.poll_nonblock

  if @poller.readables.include?(@router_socket)
    msgs = multi_recv @router_socket

    case msgs[-2]
    when "schedule"
      # for now, dumbly assume all input is plugin exec requests
      plugin_command, plugin_args = process_msg(msgs)
      pid, info = exec_plugin(plugin_command, plugin_args)
      plugins[pid] = info
    when "notification_ack"
      process_notification_ack msgs[-1] 
    else
      # log error
      hastur_send(@router_socket, "error", {:message => "Unable to deal with this type of message => #{msgs[-2]}"})
    end
  end

  msg, sender = @udp_socket.recvfrom_nonblock(100000) rescue nil  # More than max UDP packet size
  process_udp_message(msg) unless msg.nil? || msg == ""

  # If this throttles too much, adjust downward as needed
  sleep 0.1

  if Time.now - @last_heartbeat > HEARTBEAT_INTERVAL
    STDERR.puts "Sending heartbeat"
    hastur_send(@router_socket, "heartbeat", { :name => "hastur thin client", :uuid => CLIENT_UUID } )
    @last_heartbeat = Time.now
  end

  if Time.now - @last_notification_check > NOTIFICATION_INTERVAL && !@notifications.empty?
    STDERR.puts "Checking unsent notifications"
    @notifications.each_pair do |notification_id, notification|
      hastur_send(@router_socket, "notify", notification)
    end
    @last_notification_check = Time.now
  end
end

def register_client(uuid)
  # register the client
  hastur_send @router_socket, "register", {:params => { :name => CLIENT_UUID, :hostname => Socket.gethostname, :ipv4 => IPSocket::getaddress(Socket.gethostname) }, :id => CLIENT_UUID, :method => "register_client"}
  # log to hastur that we at least attempted to register this client
  hastur_send @router_socket, "logs", { :message => "Attempting to register client #{CLIENT_UUID}", :uuid => CLIENT_UUID }
end

def main
  plugins = {}
  @notifications = {}
  set_up_local_ports
  set_up_router
  set_up_poller
  register_client CLIENT_UUID

  loop do
    poll_plugin_pids(plugins)
    poll_zmq(plugins)
  end
end

main()
