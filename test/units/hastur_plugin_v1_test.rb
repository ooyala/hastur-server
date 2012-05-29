#!/usr/bin/env ruby
require_relative "../test_helper"

require 'minitest/autorun'
require 'hastur-server/agent/plugin_v1_exec'
require 'hastur-server/libc_ffi'

class TestHasturAgentPluginV1ExecModule < MiniTest::Unit::TestCase
  PLUGIN_PATH = File.join(File.dirname(__FILE__), "plugins")

  def setup
    Signal.trap("ALRM") do
      assert false, "Timed out running tests."
    end

    LibC.alarm(30)
  end

  def teardown
    LibC.alarm(0)
  end

  def run_plugin(name, args=[], should_succeed=true)
    p = Hastur::Agent::PluginV1Exec.new(File.join(PLUGIN_PATH, name), args, "my_name")

    pid = p.run
    assert pid > 1, "p.run should return a pid"

    loop do
      if p.done?
        stdout, stderr = p.slurp
        break
      else
        sleep 0.1
      end
    end

    h = p.to_hash
    assert_equal "my_name", h[:name]
  end

  def test_minimal_plugins
    # an empty plugin that just returns 0 with no output
    # perfectly valid, though not very useful
    run_plugin("minimal_plugin.sh")
    run_plugin("minimal_plugin.rb")

    # send a nonsense list of options to the script, just verify it executes
    run_plugin("minimal_plugin.sh", %w[--foo bar --baz boo -x -c -w 50 -p asdf the long frog did sing a song])
  end

  def test_basic_plugins
    # basically the same plugin in the ruby, perl, bash, and python
    run_plugin("basic_nagios_plugin.rb")
    run_plugin("basic_nagios_plugin.pl")
    run_plugin("basic_nagios_plugin.sh")
    run_plugin("basic_nagios_plugin.py")
  end

  def test_extended_plugins
    # extended hastur in ruby, perl, and shell
    run_plugin("extended_hastur_plugin.rb")
    run_plugin("extended_hastur_plugin.pl")
    run_plugin("extended_hastur_plugin.sh")
  end

  def test_pathological_plugins
    run_plugin("pathological_sleep.rb", [], false)
    run_plugin("pathological_cpu.rb", [], false)
    run_plugin("pathological_ram.rb", [], false)
    run_plugin("pathological_stdout.rb", [], false)
    run_plugin("pathological_stderr.rb", [], false)
  end
end
