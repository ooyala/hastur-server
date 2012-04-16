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
        erb :index, :locals => { :hostnames => hostnames }
      end

      get "/uuids_proxy" do
        response = get("/uuids?start=#{params[:start]}&end=#{params[:end]}")
        [ response.code, response.body ]
      end

      get "/data_proxy/*" do
        url = "http://#{@retrieval_uri}/data/stat/values?start=#{params[:start]}&end=#{params[:end]}&uuid=#{params[:uuid]}"
        url += "&name=#{params[:name]}" if params[:name]

        response = HTTParty.get(url)
        [ response.code, response.body ]
      end

      helpers do
        #
        # Calls out to the retrieval service by appending 'path' to http://<retrieval_uri>
        #
        def get( path )
          url = "http://#{@retrieval_uri}/#{path}"
          HTTParty.get(url)
        end
      end
      
    end
  end
end

