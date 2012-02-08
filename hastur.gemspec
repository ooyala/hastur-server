# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "hastur/version"

Gem::Specification.new do |s|
  s.name        = "hastur"
  s.version     = Hastur::VERSION
  s.authors     = ["Noah Gibbs"]
  s.email       = ["noah@ooyala.com"]
  s.homepage    = ""
  s.summary     = %q{A monitoring router to tame the eldritch horror of your network}
  s.description = <<EOS
Hastur is a monitoring network (like Nagios) running clients on the monitored systems,
routers to forward the information and various back-end sinks to organize and store
information about your systems.
EOS

  s.rubyforge_project = "hastur"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:
  s.add_development_dependency "scope"
  s.add_development_dependency "mocha"
  s.add_development_dependency "erubis"
  s.add_development_dependency "rainbow"
  s.add_runtime_dependency "yajl-ruby"
  s.add_runtime_dependency "multi_json"
  s.add_runtime_dependency "ffi-rzmq"
  s.add_runtime_dependency "trollop"
  s.add_runtime_dependency "uuid"
  s.add_runtime_dependency "termite"
  s.add_runtime_dependency "bluepill"
end
