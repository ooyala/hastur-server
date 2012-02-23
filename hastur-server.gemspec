# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "hastur/version"

Gem::Specification.new do |s|
  s.name        = "hastur-server"
  s.version     = Hastur::VERSION
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

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:
  s.add_development_dependency "scope"
  s.add_development_dependency "mocha"
  s.add_development_dependency "erubis"
  s.add_development_dependency "rerun"
  s.add_development_dependency "sinatra"
  s.add_development_dependency "nodule"
  s.add_runtime_dependency "yajl-ruby"
  s.add_runtime_dependency "multi_json"
  s.add_runtime_dependency "ffi-rzmq"
  s.add_runtime_dependency "trollop"
  s.add_runtime_dependency "uuid"
  s.add_runtime_dependency "termite"
  s.add_runtime_dependency "bluepill"
  s.add_runtime_dependency "cassandra"
  s.add_runtime_dependency "thrift_client", "~>0.7.0"
  s.add_runtime_dependency "rainbow"
  s.add_runtime_dependency "msgpack"
  s.add_runtime_dependency "hastur"
end
