module Hastur
  module Trigger
    class Context
      def web_hook(url, options = {})
        method = options[:method] || :get
        HTTParty.send(method, url, :query => options[:params] || nil)
      end
    end
  end
end
