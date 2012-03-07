#!/usr/bin/env ruby

require "sinatra"
require "httparty"

get "/data_proxy/*" do
  url = "http://localhost:4177/data/stat/values?start=#{params[:start]}&end=#{params[:end]}&uuid=#{params[:uuid]}"
  url += "&name=#{params[:name]}" if params[:name]
  [ 200, HTTParty.get(url) ]
end
