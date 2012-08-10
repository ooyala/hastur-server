require "hastur-server/aggregation/simple"
require "hastur-server/aggregation/merge"
require "hastur-server/aggregation/lookup"
require "hastur-server/aggregation/compound"
require "hastur-server/aggregation/formats"
require "hastur-server/aggregation/rollup"
require "hastur-server/aggregation/heuristics"

module Hastur
  module Aggregation
    class InvalidAggFunError < StandardError ; end
    class InvalidAggSyntaxError < StandardError ; end
    extend self

    # common strings that appear in v1 functions, before switching to symbol style
    ALLOWED_BARE_STRINGS = %w[ uuid name add cnt avg ]

    # series = { uuid => { name => { timestamp => value, ... } } }
    def evaluate(string, series, control)
      exp = tokenize(string)

      until exp.none?
        args = []
        until args[-1].kind_of? Symbol or exp.none?
          args.push exp.pop
        end

        fun = args.pop
        if fun.kind_of? Symbol
          series, control = self.send fun, series, control, *args.reverse
        else
          raise InvalidAggFunError.new "not a valid function: #{fun.inspect}"
        end
      end
      series
    end

    #
    # Tokenize an aggregation expression.
    #
    # @param [String] string aggregation expression in a string
    # @return [Array<String,Symbol,Fixnum,Float,Boolean,nil>] tokens
    #
    def tokenize(string)
      parts = string.split(/\s*[\(\)]\s*/).map do |token|
        token.split(/\s*,\s*/).map do |exp|
          # avoid :to_sym on random input, symbols are never gc'ed
          if @functions.has_key? exp
            @functions[exp]
          elsif exp =~ /\A[\-\+]?\d+\Z/
            exp.to_i
          elsif exp =~ /\A[\-\+]?\d*\.\d+\Z/
            exp.to_f
          elsif "true" === exp
            true
          elsif "false" === exp
            false
          elsif "null" === exp or "nil" === exp
            nil
          # symbols, but kept as strings in ruby so we don't leak bad symbols
          elsif /\A:(?<str>[-\.\w]+)\Z/ =~ exp
            str
          # compatibility with pre-symbol keywords, deprecated 2012-08-08
          elsif ALLOWED_BARE_STRINGS.include? exp
            exp
          else
            raise InvalidAggSyntaxError.new "syntax error in expression: #{exp.inspect}"
          end
        end
      end
      parts.flatten
    end
  end
end
