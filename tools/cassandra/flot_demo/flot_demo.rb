#!/usr/bin/env ruby

require "sinatra"
require "httparty"

get "/" do
  redirect "/index.html"
end

get "/uuids_proxy" do
  url = "http://localhost:4177/uuids?start=#{params[:start]}&end=#{params[:end]}"

  response = HTTParty.get(url)
  [ response.code, response.body ]
end

get "/data_proxy/*" do
  url = "http://localhost:4177/data/stat/values?start=#{params[:start]}&end=#{params[:end]}&uuid=#{params[:uuid]}"
  url += "&name=#{params[:name]}" if params[:name]

  response = HTTParty.get(url)
  [ response.code, response.body ]
end
