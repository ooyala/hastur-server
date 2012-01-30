# Handle a bunch of common Hastur CLI options & checks more succinctly.

require 'rubygems'
require 'trollop'
require 'ffi-rzmq'
require 'uuid'

module Hastur
  module CLI
    ZMQ_TYPELIST = ZMQ::SocketTypeNameMap.values.join(", ")

    OPTIONS = {
      :uri        => ["ZeroMQ URI",                                      :type => String, :multi => true, :required => true],
      :type       => ["ZMQ Socket Type, one of: #{ZMQ_TYPELIST}",        :type => String, :required => true],
      :bind       => ["bind()",           :default => false,             :type => :boolean],
      :connect    => ["connect()",        :default => false,             :type => :boolean],
      :linger     => ["set ZMQ_LINGER",   :default => 1,                 :type => Integer],
      :hwm        => ["set ZMQ_HWM",      :default => 1,                 :type => Integer],
      :uuid       => ["client UUID",                                     :type => String],
      :send       => ["send() - only for router or dealer sockets",      :type => :boolean],
      :recv       => ["recv() - only for router or dealer sockets",      :type => :boolean],
      :sleep      => ["sleep seconds",    :default => 0.1,               :type => Float],
      :spam       => ["spam 1 msg",       :default => false,             :type => :boolean],
      :infile     => ["read from <filename> instead of STDIN",           :type => String],
      :outfile    => ["append to <filename> instead of STDOUT",          :type => String],
      :subscribe  => ["subscribe pattern",:default => "",                :type => String],
      :normalize  => ["normalize JSON",   :default => false,             :type => :boolean],
      :prefix     => ["prefix string",    :default => "",                :type => String],
      :envelope   => ["envelope string",                                 :type => String, :multi => true],
      :route      => ["do Hastur client routing",                        :type => :boolean],
    }

    def initialize(want_options)
      opts = Trollop::options do
        want_options.each do |option_name|
          opt option_name, OPTIONS[option_name]
        end
      end

      if want_options.include?(:bind) and want_options.include?(:connect)
        if (opts[:bind].nil? and opts[:connect].nil?) or (opts[:bind] == opts[:connect])
          Trollop::die "Exactly one of --bind or --connect is required."
        end
      end

      if want_options.include?(:uri) 
        opts[:uri].any? do |uri|
          if uri !~ /\w+:\/\/[^:]+:\d+/
            Trollop::die :uri, "--uri must be in protocol://hostname:port form"
          end

          if opts[:uri] =~ /\Wlocalhost\W/
            Trollop::die :uri, "Don't use 'localhost'. ZMQ 2.x will break silently around IPv6 localhost."
          end
        end
      end

      if want_options.include?(:subscribe)
        # could fire with no options, but only in cases where the combination makes no sense
        if want_options.include?(:type) and opts[:type].downcase != "sub"
          Trollop::die :subscribe, "You may only use option 'subscribe' with a socket of type sub!"
        end
      end

      if want_options.include?(:type)
        unless ZMQ::SocketTypeNameMap.has_value?(opts[:type].upcase)
          Trollop::die :type, "must be one of: #{ZMQ_TYPELIST}"
        end
      end

      if block_given?
        opts.merge yield
      end

      opts
    end
  end
end

