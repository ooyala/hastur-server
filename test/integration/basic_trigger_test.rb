#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require 'scope'
require 'nodule'
require 'nodule/zeromq'
require 'nodule/alarm'
require 'multi_json'
require 'hastur-server/message'
require 'hastur-server/mock/nodule_agent'
require 'minitest/autorun'

require "ffi-rzmq"

TEST_TRIGGER = File.join(HASTUR_ROOT, "tools", "trigger", "triggers", "logging_trigger.rb")

TEST_COUNTER_ENVELOPE = Hastur::Envelope.new :type => Hastur::Message::Stat::Counter, :from => A1UUID, :to => A2UUID
TEST_COUNTER_1 = <<JSON
{
  "type": "counter",
  "value": 1,
  "name": "write.out.counter",
  "timestamp": 1335826232943810,
  "labels": {
  }
}
JSON

TEST_GAUGE_ENVELOPE = Hastur::Envelope.new :type => Hastur::Message::Stat::Gauge, :from => A1UUID, :to => A2UUID
TEST_GAUGE_1 = <<JSON
{
  "type": "gauge",
  "value": 1,
  "name": "write.out.gauge",
  "timestamp": 1335826232943810,
  "labels": {
  }
}
JSON
TEST_GAUGE_2 = <<JSON
{
  "type": "gauge",
  "value": 17,
  "name": "other.gauge",
  "timestamp": 1335826232943811,
  "labels": {
  }
}
JSON

TEST_MARK_ENVELOPE = Hastur::Envelope.new :type => Hastur::Message::Stat::Mark, :from => A1UUID, :to => A2UUID
TEST_MARK_1 = <<JSON
{
  "type": "mark",
  "value": 1,
  "name": "write.out.mark",
  "timestamp": 1335826232943810,
  "labels": {
  }
}
JSON
TEST_MARK_2 = <<JSON
{
  "type": "mark",
  "value": 31,
  "name": "other.mark",
  "timestamp": 1335826232943814,
  "labels": {
  }
}
JSON

TEST_EVENT_ENVELOPE = Hastur::Envelope.new :type => Hastur::Message::Event, :from => A1UUID, :to => A2UUID
TEST_EVENT_1 = <<JSON
{
  "type": "event",
  "name": "write.out.event",
  "timestamp": 1335826232943810,
  "labels": {
  }
}
JSON
TEST_EVENT_2 = <<JSON
{
  "type": "event",
  "name": "other.event",
  "timestamp": 1335826232943819,
  "labels": {
  }
}
JSON

TEST_HB_PROCESS_ENVELOPE = Hastur::Envelope.new :type => Hastur::Message::HB::Process, :from => A1UUID, :to => A2UUID
TEST_HB_PROCESS_1 = <<JSON
{
  "type": "hb_process",
  "name": "write.out.hb_process",
  "value": 1,
  "timestamp": 1335826232943810,
  "labels": {
  }
}
JSON

class BasicTriggerTest < Scope::TestCase
  setup_once do
    @topology = Nodule::Topology.new(
      :alarm           => Nodule::Alarm.new(:timeout => test_timeout(20)),
      :greenio         => Nodule::Console.new(:fg => :green),
      :redio           => Nodule::Console.new(:fg => :red),
      :cyanio          => Nodule::Console.new(:fg => :cyan),
      :yellow          => Nodule::Console.new(:fg => :yellow),

      :firehose        => Nodule::ZeroMQ.new(:bind => ZMQ::PUB, :uri => :gen),
      :syndicator      => Nodule::ZeroMQ.new(:bind => ZMQ::PUB, :uri => :gen),

      :syndicator_proc => Nodule::Process.new(
        TRIGGER_SYNDICATOR_BIN,
        '--firehose', :firehose,
        '--workers', :syndicator,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :worker_proc   => Nodule::Process.new(
        TRIGGER_WORKER_BIN,
        '--syndicator', :syndicator,
        '--triggers', TEST_TRIGGER,
        '--no-cassandra',
        :stdout => :capture, :stderr => :redio, :verbose => :cyanio,
      ),
    )

    @topology.start_all
  end

  teardown_once do
    @topology.stop_all
  end

  # @example count = count_messages(:event)
  def count_messages(type)
    out_array = @topology[:worker_proc].stdout
    out_array.select { |line| line =~ Regexp.new("MSG: #{type}") }.size
  end

  context "trigger messages" do
    should "be filtered properly" do
      context = ZMQ::Context.new
      socket = Hastur::Util.bind_socket(context, ZMQ::PUB, @topology[:firehose].uri, :hwm => 10_000)

      t = Time.now
      loop do
        if Time.now - t > 60
          flunk "Couldn't even start up!  #{@topology[:worker_proc].stdout.inspect}"
        end

        Hastur::Util.send_strings(socket, [TEST_COUNTER_ENVELOPE.pack, TEST_COUNTER_1])
        sleep 0.1

        contents = @topology[:worker_proc].stdout #_pipe.readlines
        #puts "Contents: #{contents.inspect}"
        break if contents.any? { |line| line =~ /MSG: counter "write.out.counter"/ }
      end

      STDERR.puts "Trigger is running!"

      sleep 1.0  # In-flight messages should clear

      pre_counter = count_messages(:counter)
      STDERR.puts "Pre-counter: #{pre_counter}"

      [
        [3, [TEST_COUNTER_ENVELOPE.pack, TEST_COUNTER_1]],
        [4, [TEST_GAUGE_ENVELOPE.pack, TEST_GAUGE_1]],
        [5, [TEST_MARK_ENVELOPE.pack, TEST_MARK_1]],
        [6, [TEST_EVENT_ENVELOPE.pack, TEST_EVENT_1]],
        [1, [TEST_HB_PROCESS_ENVELOPE.pack, TEST_HB_PROCESS_1]],
      ].each do |count, msgs|
        count.times do
          Hastur::Util.send_strings(socket, msgs)
        end
      end

      until @topology[:worker_proc].stdout.any? { |line| line =~ /^MSG: hb_process/ }
        sleep 0.25
      end

      post = {}
      [:counter, :gauge, :mark, :event].each do |type|
        post[type] = count_messages(type)
      end

      post[:counter] -= pre_counter

      assert_equal 6, post[:event], "Must see 6 events received!"
      assert_equal 5, post[:mark], "Must see 5 marks received!"
      assert_equal 4, post[:gauge], "Must see 4 marks received!"
      assert_equal 3, post[:counter], "Must see 3 counters received!"
    end
  end
end
