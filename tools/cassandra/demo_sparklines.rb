#!/usr/bin/env ruby

require "sinatra"
require "httparty"

get "/" do
  erb :demo_sparklines
end

get "/proxy" do
  url = "http://localhost:4177/data/stat/values?start=#{params[:start]}&end=#{params[:end]}"
  url += "&name=#{params[:name]}" if params[:name]
  [ 200, HTTParty.get(url) ]
end
