#!/usr/bin/env ruby

started = Time.new.to_i
stop_after = started + 7

iter = 0

loop do
  iter += 1

  if Time.new.to_i > stop_after
    break
  end
end

puts "OK - Burned 7 seconds of CPU for no good reason."
exit 0


