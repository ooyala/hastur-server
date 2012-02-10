# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "hastur_api/version"

Gem::Specification.new do |s|
  s.name        = "hastur-api"
  s.version     = HasturApi::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Viet Nguyen"]
  s.email       = ["viet@ooyala.com"]
  s.homepage    = "http://www.ooyala.com"
  s.description = "Hastur API client gem"
  s.summary     = "A gem used to communicate with the Hastur Client through UDP."
  s.rubyforge_project = "hastur-api"

  s.add_dependency "multi_json"

  s.files         = `git ls-files`.split("\n")
  s.require_paths = ["lib"]
end

