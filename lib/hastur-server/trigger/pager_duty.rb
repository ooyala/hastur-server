require "multi_json"
require "httparty"

module Hastur
  module Trigger
    class Context
      # This is a PagerDuty API key
      @@pagerduty_key = "43692e10760a012fb67b22000a9040cf"

      #
      # Sends a page to PagerDuty, currently always to the Tools and Automation project.
      #
      # @param incident_id [String] the PagerDuty incident key
      # @param msg [String] the PagerDuty incident description
      # @param json_data [Hash] additional JSON data sent as details
      # @param options [Hash] options
      # @option options [boolean] :no_create Don't really create an incident, this is test-only
      #
      def pager_duty(incident_id, msg, json_data = {}, options = {})
        # TODO(noah): Check squelches

        if @logger
          @logger.info "Paging: i_id: #{incident_id.inspect} msg: #{msg.inspect} " +
            "details: #{json_data.inspect} options: #{options.inspect}"
        end

        if options[:no_create]
          @logger.info "Not creating PagerDuty notification due to :no_create option" if @logger
        else
          @logger.info "Creating via HTTP POST to PagerDuty" if @logger
          reply = HTTParty.post "https://events.pagerduty.com/generic/2010-04-15/create_event.json",
                                :body => MultiJson.dump({
                                  :service_key => @@pagerduty_key,
                                  :incident_key => incident_id,
                                  :event_type => "trigger",
                                  :description => msg,
                                  :details => json_data,
                                })

          @logger.info "Posted, reply is #{reply.inspect}" if @logger

          unless reply.code >= 200 && reply.code < 400
            raise "Error creating PagerDuty incident: #{reply.inspect}"
          end
        end
      end
    end
  end
end
