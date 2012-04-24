require "sinatra/base"

require "httparty"

module Hastur
  module Flot
    class Dashboard < Sinatra::Base

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

