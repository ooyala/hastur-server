#
# Static configuration for Hastur Client. These configurations should
# never change because it is tightly coupled with all of the services.
#
module HasturClientConfig
  REGISTER_ROUTE="register"
  NOTIFY_ROUTE="notification"
  STATS_ROUTE="stats"
  LOG_ROUTE="log"
  ERROR_ROUTE="error"
  HEARTBEAT_ROUTE="heartbeat"
  HASTUR_CLIENT_UDP_PORT=8125
  HASTUR_CLIENT_ZMQ_ROUTERS=%w[ tcp://127.0.0.1:4321 ]
end
