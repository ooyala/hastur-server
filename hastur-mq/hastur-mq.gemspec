# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "hastur-mq/version"

Gem::Specification.new do |s|
  s.name        = "hastur-mq"
  s.version     = Hastur::Mq::VERSION
  s.authors     = ["Noah Gibbs"]
  s.email       = ["noah@ooyala.com"]
  s.homepage    = ""
  s.summary     = %q{A gateway to the Hastur message bus}
  s.description = %q{This gem abstracts the Hastur message bus, currently STOMP.}

  s.rubyforge_project = "hastur-mq"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_development_dependency "scope"
  s.add_runtime_dependency "onstomp"
  s.add_runtime_dependency "multi_json"
end
