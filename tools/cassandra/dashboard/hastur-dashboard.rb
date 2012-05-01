require "sinatra/base"

require "cgi"
require "httparty"

module Hastur
  module Flot
    class Dashboard < Sinatra::Base

      DASHBOARD_DATA_LOCATION="#{File.dirname(__FILE__)}/data"

      def initialize(retrieval_service_uri)
        @retrieval_uri = retrieval_service_uri
        super
      end
      
      get "/" do
        res = get("/hostnames").body
        hostnames = MultiJson.load(res)
        hostnames["all"] = "All"
        erb :index, :locals => { :hostnames => hostnames }
      end

      get "/hostnames" do
        res = get("/hostnames").body
        [200, res]
      end

      get "/uuids_proxy" do
        response = get("/uuids?start=#{params[:start]}&end=#{params[:end]}")
        [ response.code, response.body ]
      end

      get "/data_proxy/*" do
        url = "/data/stat/json?start=#{params[:start]}&end=#{params[:end]}&uuid=#{params[:uuid]}"
        url += "&name=#{params[:name]}" if params[:name]
        response = get(url)
       
        content_type :json
        response.body
      end

      get "/statNames" do
        [ :uuid ].each { |p| check_present p }

        res = get("/statNames?uuid=#{params[:uuid]}")

        hash = ::MultiJson.load(res.body)
        hash[params[:uuid]] << "All"
        hash[params[:uuid]].sort!

        content_type :json
        ::MultiJson.dump(hash)
      end

      get "/dashboard/:name" do
        f = File.new("#{DASHBOARD_DATA_LOCATION}/#{params[:name]}", "r")
        dashboard_data = ""
        while(line = f.gets)
          dashboard_data << line
        end

        content_type :json
        ::MultiJson.dump({ :data => dashboard_data })
      end

      get "/dashboardnames" do
        entries = Dir.entries(DASHBOARD_DATA_LOCATION)
        entries.shift   # remove '.'
        entries.shift   # remove '..'
        entries.shift   # remove '.gitkeep'
        content_type :json
        ::MultiJson.dump({ :dashboardNames => entries })
      end

      post "/dashboard/:name" do
        body = ::CGI.unescape(params[:data])
        raw_data = MultiJson.dump(body)
        STDERR.puts raw_data
        begin
          # save this data
          File.open("#{DASHBOARD_DATA_LOCATION}/#{params[:name]}", 'w+') do |f|
            f.write(raw_data)
          end
          [200]
        rescue Exception => e
          STDERR.puts e.message
          [500, e.message]
        end
      end

      helpers do
        #
        # Calls out to the retrieval service by appending 'path' to http://<retrieval_uri>
        #
        def get( path )
          url = "http://#{@retrieval_uri}/#{path}"
          HTTParty.get(url)
        end

        def check_present(param_name, human_name = nil)
          unless params[param_name]
            halt 404, "{ \"msg\": \"#{human_name || param_name} param is required!\" }"
          end
        end
      end
    end
  end
end

