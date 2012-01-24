#
# Starts a daemon that will monitor a machine. Statistics will be gathered
# by statsD and application plugins that will report numbers.
#
require "rubygems"
require "uuid"

# hastur libs
require_relative "lib/json_builder"
require_relative "lib/client_config"
require_relative "lib/hastur_heartbeats"
require_relative "lib/hastur_logger"
require_relative "lib/hastur_notification_queue"
require_relative "lib/hastur_uuid_utils"

# receivers
require_relative "receivers/hastur_message_receiver"

# hastur listeners
require_relative "listeners/hastur_listener"

class HasturClient
  attr_accessor :listeners, :uuid, :receiver

  def initialize
    @listeners = []
  end

  #
  # Prepares all of the listeners, queues, etc. necessary to bring the client up to life.
  #
  def start(uuid = nil)
    register_client(uuid)

    # start to monitor any outstanding notifications
    HasturNotificationQueue.run

    # listen on MQ for messages from Hastur
    @receiver = HasturMessageReceiver.new(HasturMessenger.socket)
    @receiver.start

    # listen for hastur traffic on a port
    @listeners << HasturListener.new(HasturClientConfig::HASTUR_CLIENT_UDP_PORT, :udp)
    HasturLogger.log("Listening on #{HasturClientConfig::HASTUR_CLIENT_UDP_PORT} for traffic")

    # periodically give a client heartbeat
    HasturHeartbeat.instance.start( 30 )   # 30 second heartbeats
  end

  #
  # Stops all of the listeners, queues, etc. Essentially ending all functionality provided for by this client
  #
  def stop
    HasturNotificationQueue.stop(true)
    @receiver.stop
    @listeners.each do |l|
      l.stop
    end
    HasturHeartbeat.instance.stop
  end

  #
  # Message via MQ to register this machine with hastur
  #
  def register_client(uuid = nil)
    if uuid.nil?
      @uuid = Hastur::Client::UuidUtils.instance.get_uuid
    else
      @uuid = uuid
    end
    register_client_req = HasturJsonBuilder.get_register_client( @uuid )
    # prepare the messenger with our uuid so he knows what to tag messages as
    HasturMessenger.set_uuid( @uuid )
    # let Hastur know that the client is alive
    HasturMessenger.send(HasturClientConfig::REGISTER_ROUTE, register_client_req)
    HasturLogger.log("Attempting to start up the client with uuid #{@uuid}")
  end
end

