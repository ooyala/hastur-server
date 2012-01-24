#!/usr/bin/env ruby

STDERR.close # TODO: reopen /dev/null
# more evil tests aren't terribly practical at this point
# though it might be fun to read /dev/urandom or /dev/zero and
# make sure it doesn't crash anything

sleep 1

puts "OK - closed STDERR early"
exit 0

