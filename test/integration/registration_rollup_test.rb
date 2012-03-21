#!/usr/bin/env ruby

require 'minitest/autorun'
require 'multi_json'

require_relative "./integration_test_helper"

require 'hastur'
require 'hastur-server/message'
require 'hastur-server/util'

require 'nodule/cassandra'
require 'nodule/console'
require 'nodule/process'
require 'nodule/topology'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'nodule/util'

class RegistrationRollupTest < MiniTest::Unit::TestCase

  FAKE_UUID = "fafafafa-fafa-fafa-fafa-fafafafafafa"

  def setup
    set_test_alarm(30) # helper

    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :cassandra     => Nodule::Cassandra.new( :keyspace => "Hastur", :verbose => :greenio ),
      :reg_rollup    => Nodule::Process.new(
        HASTUR_REGISTRATION_ROLLUP_BIN,
        'hosts', :cassandra
      ),
    )

    # start cassandra
    @topology.start :cassandra
    create_all_column_families(@topology[:cassandra]) # helper

    # start everything else but the registration rollup
    @topology.keys.each do |key|
      if key.to_s != "reg_rollup" && key.to_s != "cassandra"
        @topology.start key.to_sym
      end
    end
  end
  
  def teardown
    @topology.stop_all
  end
 
  def test_rollup
    client = @topology[:cassandra].client

    # make sure the cassandra schema is at least loaded
    assert client.get(:RegistrationArchive, "key") != nil
    assert client.get(:RegistrationDay, "key") != nil
    assert_equal "Hastur", @topology[:cassandra].keyspace

    # TODO(viet): pump data into C*
    data=<<JSON
      {
        "type"        : "plugin",
        "plugin_path" : "echo",
        "plugin_args" : "OK",
        "interval"    : "five_minutes",
        "plugin"      : "my.plugin.echo",
        "timestamp"   : #{Hastur::Util.timestamp}
      }
JSON

    ::Hastur::Cassandra.insert(client, data, "registration", { :uuid => FAKE_UUID })

    # start the registration rollup once we know cassandra is up and running
    @topology.start :reg_rollup

    # TODO(viet): check rollup CF to make sure data is correctly rolled up
  end
end
