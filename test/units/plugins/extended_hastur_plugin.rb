#!/usr/bin/env ruby

require 'multi_json'

report = {
  :status  => "OK",
  :exit    => 0,
  :message => "ruby extended plugin works fine",
  :stats   => [
    { :runtime => 0.0, :units => "s" },
  ],
  :tags => ["version_0.1", "ruby", "hastur"],
}

puts MultiJson.dump(report)
exit 0

