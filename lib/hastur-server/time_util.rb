require "date"

module Hastur
  #
  # A collection of utility methods for handling microsecond time.
  #
  # @example
  #   class Foo
  #     include Hastur::TimeUtil
  #   end
  #
  module TimeUtil
    extend self

    # Time interval constants. Fluxuations in wall clock time are ignored.  A week is 7 days, a day is 24 hours.
    # No leap seconds, daylight savings time, or any of that kind of nonsense.
    USEC_ONE_SECOND   = 1_000_000
    USEC_ONE_MINUTE   = 60 * USEC_ONE_SECOND
    USEC_FIVE_MINUTES =  5 * USEC_ONE_MINUTE
    USEC_ONE_HOUR     = 60 * USEC_ONE_MINUTE
    USEC_ONE_DAY      = 24 * USEC_ONE_HOUR
    USEC_ONE_WEEK     =  7 * USEC_ONE_DAY

    # epoch time constants for use in best-effort conversion of various
    # other timestamp formats into microseconds since Jan 1 1970
    SECS_2100       = 4102444800
    MILLI_SECS_2100 = 4102444800000
    MICRO_SECS_2100 = 4102444800000000
    NANO_SECS_2100  = 4102444800000000000
    SECS_1971       = 31536000
    MILLI_SECS_1971 = 31536000000
    MICRO_SECS_1971 = 31536000000000
    NANO_SECS_1971  = 31536000000000000

    #
    # Best effort to make all timestamps be Hastur timestamps, 64 bit
    # numbers that represent the total number of microseconds since Jan
    # 1, 1970 at midnight UTC.  Default to giving Time.now as a Hastur
    # timestamp.
    #
    # @param [Time, Fixnum] ts default Time.now
    # @return [Fixnum] epoch microseconds
    #
    def usec_epoch(ts=Time.now)
      case ts
        when nil, ""
          (Time.now.to_f * 1_000_000).to_i
        when Time
          (ts.to_f * 1_000_000).to_i
        when SECS_1971..SECS_2100
          ts * 1_000_000
        when MILLI_SECS_1971..MILLI_SECS_2100
          ts * 1_000
        when MICRO_SECS_1971..MICRO_SECS_2100
          ts
        when NANO_SECS_1971..NANO_SECS_2100
          ts / 1_000
        else
          raise "Unable to convert timestamp: #{ts} (class: #{ts.class})"
      end
    end

    #
    # Truncate a microsecond timestamp down to seconds (unix epoch).
    #
    # @param [Fixnum] ts
    # @return [Fixnum] unix epoch seconds
    #
    def usec_to_sec(ts)
      ts / USEC_ONE_SECOND
    end

    def usec_to_time(ts)
      Time.at(ts / USEC_ONE_SECOND).utc
    end

    #
    # Truncate a microsecond timestamp on the given boundary. It is always truncated into the past.
    #
    # Anything smaller than a day is passed through to usec_truncate_subday().
    # Anything bigger than a day is converted to a Ruby Date and converted back. These values will
    # take into account all the variations in wallclock time that Ruby's Date class does.
    # :day is aligned to midnight UTC.
    # :week is aligned to Sunday morning at midnight UTC.
    # :month is aligned to the 1st of the month of the given time at midnight UTC.
    # :year is aligned to the 1st of January in the year of the given time at midnight UTC.
    #
    # The truncation boundary can be expressed in seconds or with one of the following symbols:
    # :second :minute :five_minutes :hour :day :week :month :year
    #
    # Only the symbols for :day/:week/:month/:year will use proper date math.
    #
    # @param [Symbol,Fixnum] timestamp
    # @return [Fixnum] truncated microsecond epoch timestamp
    # @raise [StandardError] will raise an exception for Fixnum granularity > one day
    #
    # @example
    #   usec_truncate usec_epoch, :one
    #   usec_truncate 1337371464937496, USEC_ONE_MINUTE * 30 # 1/2 hr
    #
    def usec_truncate(timestamp, boundary)
      date = usec_to_time(timestamp).to_date

      case boundary
      when :day, :one_day, USEC_ONE_DAY
        Time.utc(date.year, date.month, date.day).to_i * USEC_ONE_SECOND
      when :week, :one_week, USEC_ONE_WEEK
        start_of_week = date - date.wday # Sunday
        Time.utc(start_of_week.year, start_of_week.month, start_of_week.day).to_i * USEC_ONE_SECOND
      when :month, :one_month
        Time.utc(date.year, date.month, 1).to_i * USEC_ONE_SECOND
      when :year, :one_year
        Time.utc(date.year, 1, 1).to_i * USEC_ONE_SECOND
      when :hour, :one_hour, USEC_ONE_HOUR
        usec_truncate_subday timestamp, USEC_ONE_HOUR
      when :five_minutes, USEC_FIVE_MINUTES
        usec_truncate_subday timestamp, USEC_FIVE_MINUTES
      when :minute, :one_minute, USEC_ONE_MINUTE
        usec_truncate_subday timestamp, USEC_ONE_MINUTE
      when :second, :one_second, USEC_ONE_SECOND
        usec_truncate_subday timestamp, USEC_ONE_SECOND
      when 0..USEC_ONE_DAY
        usec_truncate_subday timestamp, boundary
      else
        raise "Cannot reliably truncate to a meaningful interval larger than one day. (#{boundary})"
      end
    end

    #
    # Truncate times smaller than one day, relative to midnight UTC.
    #
    # @param [Fixnum] timestamp time to truncate
    # @param [Fixnum] chunk_usec microseconds chunk size to align to
    # @return [Fixnum] timestamp truncated timestamp in microseconds
    #
    def usec_truncate_subday(timestamp, chunk_usec)
      # truncate the day to midnight
      date_usec = usec_truncate timestamp, :day

      # how many microseconds we are into the day
      usecs_into_day = timestamp - date_usec

      # how many chunks have passed since midnight
      chunks_into_day = usecs_into_day / chunk_usec

      # how many microseconds into the day will the new time be
      delta_usec = chunks_into_day * chunk_usec

      # final time is the delta added back to midnight then forcibly truncated to one second resolution
      ((date_usec + delta_usec) / USEC_ONE_SECOND) * USEC_ONE_SECOND
    end

    #
    # Create a list of chunk timestamps aligned on the given chunk size. Both times will be truncated
    # down to the chunk boundary, so you will pick up time on the start and lose some on the end unless
    # they're already aligned.  If the start/end are in the same chunk, that one chunk is returned. If
    # they cross a single boundary, the two chunks will be returned, and so on.
    #
    # If the end is exactly on the chunk boundary, the chunk starting with that timestamp will be included
    # in the list. If you want non-inclusive, use boundary value - 1usec.
    #
    # If what you need is month of 5 minute chunks:
    # @example usec_aligned_chunks ts - USEC_ONE_DAY * 30, ts, :five_minutes
    #
    # @example
    #   usec_aligned_chunks(start_ts, end_ts, :five_minutes).each do { |c| }
    #   usec_aligned_chunks(start_ts - USEC_FIVE_MINUTES, end_ts + USEC_FIVE_MINUTES, :five_minutes)
    #
    def usec_aligned_chunks(start_ts, end_ts, chunk)
      chunk_usec = usec_from_interval chunk
      start_trunc = usec_truncate start_ts, chunk
      end_trunc = usec_truncate end_ts, chunk

      # will be 0 if start and end truncate to the same time, indicating they don't cross a chunk boundary
      chunks = (end_trunc - start_trunc) / chunk_usec

      # always return at least the first chunk
      out = [start_trunc]

      chunks.times do
        out << out[-1] + chunk_usec
      end

      out
    end

    #
    # Turn a string or number into a number of usecs.  If a number is passed in, it's
    # assumed to be usecs and returned unmodified.
    #
    # @param [String,Symbol,Fixnum] interval string/symbol name or a number
    # @return [Fixnum] microseconds
    # @example
    #   usec_from_interval :one_minute  # one minute in usecs
    #   usec_from_interval "one_minute" # same thing
    #   usec_from_interval 1_000_000    # one second
    #   usec_from_interval "1000000"    # same thing, uses RE match!
    #
    def usec_from_interval(interval)
      case interval
      when Fixnum ; interval
      when :second, :one_second, "one_second" ; USEC_ONE_SECOND
      when :minute, :one_minute, "one_minute" ; USEC_ONE_MINUTE
      when :five_minutes, "five_minutes"      ; USEC_FIVE_MINUTES
      when :hour, :one_hour, "one_hour"       ; USEC_ONE_HOUR
      when :day, :one_day, "one_day"          ; USEC_ONE_DAY
      when :week, :one_week, "one_week"       ; USEC_ONE_WEEK
      when /\A\d+\Z/ ; interval.to_i
      end
    end
  end
end
