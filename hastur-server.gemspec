# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "hastur-server/version"

Gem::Specification.new do |s|
  s.name        = "hastur-server"
  s.version     = Hastur::SERVER_VERSION
  s.authors     = ["Noah Gibbs"]
  s.email       = ["noah@ooyala.com"]
  s.homepage    = ""
  s.summary     = %q{A monitoring system to tame the eldritch horror of your network}
  s.description = <<EOS
Hastur is a monitoring network, so it's a bit like Nagios.  You run
clients on the monitored systems, routers to forward the information
and various back-end sinks to organize and store information about
your systems.  Hastur tracks registrations, statistics and metrics,
notifications and errors, log entries and plugins.
EOS

  s.rubyforge_project = "hastur-server"
  s.required_ruby_version = ">= 1.9.3"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").
                      delete_if { |f| f =~ /\.(ecology|init|pill)$/ }.map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:
  s.add_development_dependency "scope"
  s.add_development_dependency "mocha"
  s.add_development_dependency "erubis"
  s.add_development_dependency "rerun"
  s.add_development_dependency "nodule", "~>0.0.32"
  s.add_development_dependency "yard"
  s.add_development_dependency "redcarpet"
  s.add_development_dependency "simplecov"
  s.add_development_dependency "test-unit", "~>2.4.3"
  s.add_development_dependency "rack-test"
  s.add_development_dependency "minitest", "~>3.2.0"

  if RUBY_PLATFORM == "java"
    s.add_development_dependency "jruby_astyanax-jars"
    s.add_development_dependency "warbler"
  end

  s.add_runtime_dependency "sinatra"
  s.add_runtime_dependency "grape"   # TODO: remove
  s.add_runtime_dependency "httparty"
  s.add_runtime_dependency "multi_json", "~>1.3.2"
  s.add_runtime_dependency "ffi-rzmq"
  s.add_runtime_dependency "trollop"
  s.add_runtime_dependency "uuid"
  s.add_runtime_dependency "termite"
  s.add_runtime_dependency "bluepill"
  s.add_runtime_dependency "rainbow"
  s.add_runtime_dependency "hastur", "~>1.2.8"
  s.add_runtime_dependency "pony"
  s.add_runtime_dependency "pry"
  s.add_runtime_dependency "ohai"
  s.add_runtime_dependency "sys-uname"
  s.add_runtime_dependency "hastur-rack", "~>0.0.10"

  if RUBY_PLATFORM == "java"
    s.add_runtime_dependency "jruby-msgpack" if RUBY_PLATFORM == "java"
    s.add_runtime_dependency("jruby-astyanax", "~>0.0.4") if RUBY_PLATFORM == "java"
  else
    s.add_runtime_dependency "yajl-ruby" unless RUBY_PLATFORM == "java"
    s.add_runtime_dependency("cassandra", "~>0.15") unless RUBY_PLATFORM == "java"
    s.add_runtime_dependency("thrift_client", "=0.8.1") unless RUBY_PLATFORM == "java" # 0.8.2 loses data!
    s.add_runtime_dependency "msgpack" unless RUBY_PLATFORM == "java"
    s.add_runtime_dependency "unicorn" unless RUBY_PLATFORM == "java"
  end
end
