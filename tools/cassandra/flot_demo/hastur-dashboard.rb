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
        [200, "Flot Dashboard"]
      end

      get "/uuids_proxy" do
        url = "http://#{@retrieval_uri}/uuids?start=#{params[:start]}&end=#{params[:end]}"

        response = HTTParty.get(url)
        [ response.code, response.body ]
      end

      get "/data_proxy/*" do
        url = "http://#{@retrieval_uri}/data/stat/values?start=#{params[:start]}&end=#{params[:end]}&uuid=#{params[:uuid]}"
        url += "&name=#{params[:name]}" if params[:name]

        response = HTTParty.get(url)
        [ response.code, response.body ]
      end
      
    end
  end
end

