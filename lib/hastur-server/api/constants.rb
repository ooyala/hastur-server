module Hastur
  module API
    module Constants
      extend self

      #
      # All of the Hastur message types. These are used in various places in the API
      # usually in the :type field. The keys may be used to indicate that you want all
      # of the values, so for example, "stat" will get you all counters, gauges, and marks.
      #
      TYPES = {
        :metric       => %w[counter gauge mark compound],
        :heartbeat    => %w[hb_process hb_agent hb_pluginv1],
        :event        => %w[event],
        :log          => %w[log],
        :error        => %w[error],
        :registration => %w[reg_agent reg_process reg_pluginv1],
        :info         => %w[info_agent info_process info_ohai],
        :all          => %w[counter gauge mark compound
                            hb_process hb_agent hb_pluginv1
                            event log error
                            reg_agent reg_process reg_pluginv1
                            info_agent info_process info_ohai]
      }.freeze
      FORMATS = %w[message value count rollup csv].freeze

      # TODO(al) use the schema to build these lists
      TYPES_WITH_VALUES = ["metric", TYPES[:metric], "heartbeat", TYPES[:heartbeat]].flatten.freeze
      DEFAULT_DAY_BUCKET = ["registration", TYPES[:registration], "info", TYPES[:info]].flatten.freeze
      ROLLUP_PERIODS = %w[five_minutes one_hour one_day]

      # basic checking on uuid lists
      UUID_RE = /\A[a-fA-F0-9\-,]{36,}\Z/
      UUID_OR_HOST_RE = /\A[\w,\.\-]\Z/

      THRIFT_OPTIONS = {
        :timeout => 300,
        :connect_timeout => 30,
        :retries => 10,
      }
    end
  end
end
