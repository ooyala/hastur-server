#!/usr/bin/env perl

use strict;
use warnings;
use JSON;

my $report = {
  status  => "OK",
  exit    => 0,
  message => "perl extended plugin works fine",
  stats   => [
    { runtime => 0.0, units => "s" }
  ],
  tags => ["version_0.1", "perl", "hastur"]
};

print encode_json($report), "\n";
exit 0

