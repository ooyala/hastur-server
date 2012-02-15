module Hastur
  module Test
    class ProcessNotRunningError < StandardError; end
    class ProcessAlreadyRunningError < StandardError; end
    class ProcessStillRunningError < StandardError; end

    class Process
      attr_reader :argv, :pid
      attr_reader :stdin, :stdout, :stderr

      def initialize(resources, opts={}, *argv)
        @argv = argv.flatten.map do |arg|
          if arg.kind_of? Symbol and resources.has_key? arg
            resources[arg].to_s
          else
            arg.to_s
          end
        end

        @mutex = Mutex.new
        @threads = []
        @stdout_handler = _stdio_arg(opts, :stdout, resources)
        @stderr_handler = _stdio_arg(opts, :stdout, resources)
      end

      def _stdio_arg(opts, key, resources)
        out = nil
        if opts[key].kind_of? Proc
          out = opts[key]
        elsif opts[key]
          if resources[opts[key]]
            # only one proc is supported, assume there's only one
            out = resources[opts[key]].actions[0]
          else
            raise ArgumentError.new "Invalid value for :#{key} '#{opts[:key].inspect}'"
          end
        end
      end

      def run
        raise ProcessAlreadyRunningError.new if @pid

        @stdin_r, @stdin    = IO.pipe
        @stdout,  @stdout_w = IO.pipe
        @stderr,  @stderr_w = IO.pipe

        STDERR.puts "Gonna spawn(#{@argv})"

        @pid = spawn(*@argv,
          :in  => @stdin_r,
          :out => @stdout_w,
          :err => @stderr_w,
        )

        if @stdout_handler.respond_to? :call
          @threads << Thread.new do
            begin
              @stdout.lines { |line| @mutex.synchronize { @stdout_handler.call(line) } }
            rescue 
              STDERR.puts $!.inspect, $@
            end
          end
        end

        if @stderr_handler.respond_to? :call
          @threads << Thread.new do
            begin
              @stderr.lines { |line| @mutex.synchronize { @stderr_handler.call(line) } }
            rescue 
              STDERR.puts $!.inspect, $@
            end
          end
        end

        @stdin_r.close
        @stdout_w.close
        @stderr_w.close
      end

      def puts(*args)
        @stdin.puts *args
      end

      def slurp
        raise ProcessStillRunningError.new "Cannot slurp() until the process is done." unless done?
        stdout = @stdout_handler.respond_to? :call ? nil : @stdout.lines
        stderr = @stderr_handler.respond_to? :call ? nil : @stderr.lines
        return stdout, stderr
      end

      def reset
        raise ProcessNotRunningError.new unless @pid
        @pid = nil
        @stdin.close
        @stdout.close
        @stderr.close
      end

      def kill(sig)
        raise ProcessNotRunningError.new unless @pid

        #begin
        #  ::Process.kill(sig, @pid)
        #rescue Errno::ESRCH
        #  STDERR.puts "No such process (#{@pid}) to kill."
        #end
      end

      def stop!
        raise ProcessNotRunningError.new unless @pid

        pid, status = ::Process.waitpid2(@pid, ::Process::WNOHANG)

        if pid.nil? or pid == -1
          raise ProcessNotRunningError.new "no such child process at pid #{@pid} (waitpid said: #{pid}, #{status})"
        elsif pid == @pid
          return
        else
          kill "TERM"
          sleep 0.02

          pid, status = ::Process.waitpid2(@pid, ::Process::WNOHANG)

          if pid != @pid
            kill -9
            sleep 0.02
            pid, status = ::Process.waitpid2(@pid, ::Process::WNOHANG)
          end
        end
      end

      def status
        begin
          ::Process.waitpid2(@pid, ::Process::WNOHANG)
        rescue Errno::ECHILD
          return nil, nil
        end
      end

      def done?
        return true if @pid.nil?
        pid, st = status
        pid == @pid
      end
    end
  end
end
