
module Hastur
  module Plugin
    class V1
      attr_accessor :command, :args, :out_fd, :err_fd, :pid, :status

      def initialize(command, args=[])
        @command = command
        @args    = args
        @out_fd, @out_w = IO.pipe
        @err_fd, @err_w = IO.pipe
      end

      def run(opts={})
        opts[:rlimit_cpu] ||= 10
        opts[:rlimit_as]  ||= 2**26 # 64MB of memory

        @pid = Kernel.spawn(@command, *@args, 
          :out => @out_w,
          :err => @err_w,
          :rlimit_cpu => opts[:rlimit_cpu],
          :rlimit_as  => opts[:rlimit_as],
        )

        # must happen after the fork/exec or these pipes would be useless
        @out_w.close
        @err_w.close

        @pid
      end

      # will block if called before the process has closed its stdout/stderr or exited
      # call obj.done? to check
      def slurp
        out = @out_fd.readlines
        err = @err_fd.readlines
        return out, err
      end

      def done?
        begin
          pid, status = Process.waitpid2(@pid, Process::WNOHANG)

          if pid == @pid
            @status = status
            return true
          end
        rescue Errno::ECHILD
          return false
        end

        false
      end
    end
  end
end

