require "pony"

module Hastur
  module Trigger
    class Context
      #
      # Send email to alert about a problem, error or other remarkable data feature.
      #
      # @param recipients [String or Array<String>] Who to send to
      # @param subject [String] Email subject line
      # @param body [String] Optional email body
      # @param opts [Hash] Options
      # @option opts [String or Array] :cc
      # @option opts [String or Array] :bcc
      # @option opts [String] :from
      #
      def send_email(recipients, subject, body = "", opts = {})
        recipients = [recipients].flatten
        from = opts[:from] || "Hastur Triggers"
        cc = [opts[:cc]].flatten || []
        bcc = [opts[:bcc]].flatten || []

        ret = Pony.mail :to => recipients.join(";"),
               :cc => cc.join(";"),
               :bcc => bcc.join(";"),
               :subject => subject,
               :body => body,
               :via => :sendmail

        # Could set :via_options to set sendmail
        # location and arguments.
      end
    end
  end
end
