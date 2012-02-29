module Request
  def self.handle(headers, request_path, body)
    [ 200, {}, "Hello, World!" ]
  end
end
