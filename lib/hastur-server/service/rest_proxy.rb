require "multi_json"
require 'goliath'
require 'em-synchrony/em-http'
require "hastur/eventmachine"

MultiJson.use :yajl

module Hastur
  module Service
    class RestProxy < Goliath::API
      use Goliath::Rack::Params
      use Goliath::Rack::JSONP

      attr_reader :backend
      def initialize
        ::ARGV.each_with_index do |arg,idx|
          if arg == "--backend"
            @backend = ::ARGV[idx + 1]
            ::ARGV.slice! idx, 2
            break
          end
        end

        unless @backend
          raise "Initialization error: could not determine backend server, try --backend <url>"
        end

        super
      end

      #
      # These routes are proxy-specific:
      #
      # /api/*/datatable - converts to Google DataTable format
      # /api/*/ordered   - converts to an array-of-hashes format for strong ordering
      # /api/*/two_lists - converts to separate arrays of timestamps & values
      #
      # These are proxy-specific query parameters and will not be passed through:
      #
      # @param callback Set a callback for JSONP, return JSONP format
      # @param pretty Return the JSON in pretty-print / indented format
      #
      # The following query parameters are passed through to the retrieval service:
      #
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      # @param consistency Cassandra read consistency
      #
      #
      def response(env)
        url = env['REQUEST_PATH'].split '/'
        url[0] = @backend # first element is always empty

        format = nil
        case url.last
          when 'two_lists', 'ordered'
            format = url.pop
            url.push 'value'
          when 'datatable'
            format = url.pop
            url.push 'message'
        end

        # query parameters to be passed to the retrieval service are whitelisted
        req_params = {}
        %w[start end ago limit reversed consistency].each do |p|
          if env.params.has_key? p
            req_params[p] = env.params[p]
          end
        end

        json_options = { :pretty => false }
        if param_is_true("pretty", env.params)
          json_options[:pretty] = true
        end

        req = EM::HttpRequest.new(url.join('/')).get query: req_params

        headers = { 'X-Goliath' => 'Proxy', 'X-Hastur-Format' => format }
        content = MultiJson.load req.response

        if format and not req.error
          content = format_data(format, content, env.params)
        end

        # the JSONP middleware will convert JSON if it sees the 'callback' param
        headers['Content-Type'] = "application/json"
        json = MultiJson.dump(content, json_options) + "\n"

        [req.response_header.status, headers, json]
      end

      #
      # Reformat data structures before serialization.
      # @todo document this in the regular query paths
      #
      # "two_lists" keeps the uuid/name key levels but splits timestamp / values into two lists
      #   { uuid => { name => { :timestamps => [...], :values => [...] } } }
      #
      # "ordered" transforms the hash of { timestamp => value } to an array of hashes
      # this is handy if you run into JSON parsers that don't preserve order (the spec says it's unordered)
      #   { uuid => { name => [ { :timestamp => ts, :value => val }, ... ] } }
      #
      # "sample=<int>" sets a maximum number of samples to return, it does _not_ roll up, it figures out how
      # many entries to skip and selects only every (total / samples) point out of the data. This works with
      # either of the above options or all by itself.
      #
      # @param [Hash] content
      # @return [Hash] content
      #
      def format_data(format, content, params)
        types = content.delete "types"

        # expand compound tables into names
        unless format == "message" or param_is_true("raw", params)
          reformat_compound_values(content, types, params)
        end

        # downsample the data
        if param_is_true("sample", params)
          resample_data(content, types, params)
        end

        # two_lists / ordered / datatable are mutually exclusive
        case format
        when "two_lists"
          reformat_to_two_lists(content, types, params)
        when "ordered"
          reformat_to_ordered_format(content, types, params)
        when "datatable"
          reformat_to_google_datatable(content, types, params)
        else
          content["types"] = types
          content
        end
      end

      #
      # Wrap up all the outer boilerplate loops for reformatting the datastructures
      # that come back from cassandra.
      #
      # @param [Hash] content
      # @param [Hash] types
      # @yield key, name
      # @yieldreturn [Object] data to be placed under output[key][name]
      #
      def reformat_output(content, types)
        output = {}

        content.keys.each do |key|
          if content[key].respond_to? :keys
            output[key] = {}
            content[key].keys.each do |name|
              output[key][name] = yield key, name
            end
          else
            output[key] = content[key]
          end
        end

        output[:types] = types
        output
      end

      # workaround: we're getting overlaps or unsorted data at this point that causes
      # the series to be jacked up due to out-of-order items, this re-sorts the keys on
      # each row but shouldn't end up doing a lot of copying since it reuses the value reference
      def reorder_data(content, types, params)
        reformat_output content, types do |key, name|
          row = {}
          old = content[key].delete name
          old.keys.sort.each do |ts|
            row[ts] = old[ts]
          end

          content[key][name] = row
        end
      end

      # only reformat compound entries on /value, and never if ?raw is specified
      # "explodes" compound values into multiple stats with the original keys as extra .foo on the name
      # E.g. /proc/stats goes from a big hash with cpu, cpu0, etc. in it
      # to linux.proc.stat.cpu, linux.proc.stat.cpu0, etc..
      def reformat_compound_values(content, types, params)
        reformat_output content, types do |key, name|
          if types[key][name] == "compound"
            # modify content in-place to make it a little faster and (maybe) save memory
            content[key][name].each do |timestamp, values|
              if values.respond_to? :keys
                values.each do |inner_name, inner_value|
                  newname = [name, inner_name].join('.')
                  types[key][newname] = "compound"
                  content[key][newname] ||= {}
                  content[key][newname][timestamp] = inner_value
                end
              else
                content[key][name] = values
              end
            end
            # remove original data
            content[key].delete(name)
            types[key].delete(name)
          end
        end
      end

      # modify content in-place and reduce it to the requested number of samples
      def resample_data(content, types, params)
        sample = params["sample"].to_i # rescue hastur_error(404, "sample must be an integer")

        content["sample"] = sample
        content["original_count"] = content["count"]

        # only sample the data if the number of samples is less than the original number of values
        if sample < content["count"]
          sample_every = (content["count"] / sample).to_i

          reformat_output content, types do |key, name|
            count = 0
            content[key][name].keys.each do |timestamp|
              if count % sample_every != 0
                content[key][name].delete timestamp
              end
              count = count + 1
            end

            content["count"] = count
          end
        end
      end

      # { uuid => { name => { :timestamps => [...], :values => [...] } } }
      def reformat_to_two_lists(content, types, params)
        reformat_output content, types do |key, name|
          row = { :timestamps => [], :values => [] }
          content[key][name].each do |timestamp, value|
            row[:timestamps] << timestamp
            row[:values] << value
          end
          row
        end
      end

      # { uuid => { name => [ { :timestamp => ts, :value => val }, ... ] } }
      def reformat_to_ordered_format(content, types, params)
        reformat_output content, types do |key, name|
          row = []
          content[key][name].each do |timestamp, value|
            row << { :timestamp => timestamp, :data => value }
          end
          row
        end
      end

      # Google DataTable format, for use with Google Charts
      # https://developers.google.com/chart/interactive/docs/dev/implementing_data_source#jsondatatable
      # Right now this only handles 'message' types so it can pull out the labels and pass them through.
      # It remains to be seen if that's useful or if it should be some kind of side-band.
      def reformat_to_google_datatable(content, types, params)
        out = {
          :cols => [
            { :id => :time,  :label => "Time",  :type => :datetime },
            { :id => :value, :label => "Value", :type => :number },
          ],
          :rows => []
        }

        timestamps = nil
        seen_labels = {}
        reformat_output content, types do |key, name|
          content[key][name].each do |ts, val|
            if val.respond_to?(:has_key?) and val.has_key?("labels")
              seen_labels.merge! val["labels"]
            end
          end

          if timestamps.nil?
            timestamps = content[key][name].keys.sort.map do |ts|
              t = Time.at ts.to_f/1_000_000
              # The Date() format is google-specific
              "Date(#{[t.year, t.month, t.mday, t.hour, t.min, t.sec, t.usec / 1000].join(',')})"
            end
          end
        end

        labels = seen_labels.keys.sort

        labels.each do |label|
          type = seen_labels[label].kind_of?(Numeric) ? :number : :string
          out[:cols] << { :id => "label-#{label}", :label => label, :type => type }
        end

        types.keys.each do |uuid|
          content[uuid].each do |name, series|
            series.values.each_with_index do |val, idx|
              row = [ { :v => timestamps[idx] } ]
              if %w[gauge counter mark].include?(types[uuid][name])
                row << { :v => val["value"] }
              else
                # TODO(al) 2012-06-07 handle compound types for datatable
                STDERR.puts "TODO(al): handle #{types[uuid][name]} types"
              end

              labels.each do |label|
                label_value = val["labels"][label] rescue ""
                row << { :v => label_value }
              end

              out[:rows] << { :c => row }
            end
          end
        end

        { :status => "ok", :reqId => 0, :table => out }
      end

      def param_is_true(name, params)
        params[name] && !["", "0", "false", "no", "f"].include?(params[name].downcase)
      end
    end
  end
end
