#!/usr/bin/env ruby

require "rubygems"
require 'hastur-server/plugin/v1'

def plugin(name, args)
  plugin_path = File.join(File.dirname($0), "plugins")
  p = Hastur::Plugin::V1.new(File.join(plugin_path, name), args)
  p.run
  p
end

def test_plugin(name, args)
  puts "Testing #{name} ..."
  p = plugin(name, args)

  loop do
    if p.done?
      stdout, stderr = p.slurp
      puts "#{name} exited with #{p.status} and output: #{stdout}"
      break
    end
  end
end

# an empty plugin that just returns 0 with no output
# perfectly valid, though not very useful
test_plugin("minimal_plugin.sh", [])
test_plugin("minimal_plugin.rb", [])

# basically the same plugin in the ruby, perl, bash, and python
test_plugin("basic_nagios_plugin.rb", [])
test_plugin("basic_nagios_plugin.pl", [])
test_plugin("basic_nagios_plugin.sh", [])
test_plugin("basic_nagios_plugin.py", [])

# extended hastur in ruby, perl, and shell
test_plugin("extended_hastur_plugin.rb", [])
test_plugin("extended_hastur_plugin.pl", [])
test_plugin("extended_hastur_plugin.sh", [])

test_plugin("pathological_sleep.rb", [])
test_plugin("pathological_cpu.rb", [])
test_plugin("pathological_ram.rb", [])
test_plugin("pathological_stdout.rb", [])
test_plugin("pathological_stderr.rb", [])

# send a nonsense list of options to the script, just verify it executes
test_plugin("minimal_plugin.sh", %w[--foo bar --baz boo -x -c -w 50 -p asdf the long frog did sing a song])

