require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'termite'

require "hastur-server/util"
require "hastur-server/zmq_util"

module Hastur
  module Service
    class BlockSupervisor
      STOP_TIMEOUT = 5
      LOOP_SLEEP = 0.5
      attr_reader :ruby, :harness

      def initialize(opts = {})
        @running    = false
        @ruby       = opts[:ruby]
        @harness    = opts[:harness]
        @ctx        = opts[:ctx] || ZMQ::Context.new
        @rpc_server = Hastur::RPC::Server.new @uri
        @blocks     = {}
        @processes  = {}
        @stopping   = {}
        @block_uris = {}

        @rpc_server.add_handler :add_block do |data|
          @mutex.synchronize do
            pid = launch_process data[:block_path], data[:uri]
          end
        end

        @rpc_server.add_handler :stop_block do |data|
          @mutex.synchronize do
            stop_process data[:block_path]
          end
        end

        @rpc_server.add_handler :status do
          @mutex.synchronize do get_status end
        end
      end

      def run
        @rpc_thread = Thread.new do @rpc_server.run end
        process_monitor
      end

      def stop
        @running = false
        @processes.each do |pid,info|
          Process.kill :QUIT, pid
          Process.waitpid2 pid
        end
        @rpc_thread.join
      end

      def process_monitor
        while @running
          @processes.each do |pid,block_id|
            # check & reap processes
            rc, @status = Process.waitpid2 pid, Process::WNOHANG

            # process died
            if @status and rc == pid
              unless @stopping[pid]
                info = @processes.delete pid
                @logger.info "Restarting block '#{info[1]}' at #{Time.now} after it ran for #{Time.now - info[0]} seconds."
                restart @processes[info[1]]
              end
            elsif @stopping[pid] and (@stopping[pid][0].to_i + STOP_TIMEOUT) < Time.now
              # will be reaped on next round
              Process.kill :KILL, pid
            end
          end
          sleep LOOP_SLEEP
        end
      end

      def launch_process(block_path, uri)
        pid = fork
        if pid == 0
          Kernel.exec @ruby, '--queue', uri, '--triggers', block_path
        else
          @processes[pid] = [ Time.now, block_path ]
        end
      end

      def stop_process(block_path)
        if pid = @processes.key(block_path)
          @stopping[pid] = [ Time.now, block_path ]
          Process.kill :QUIT, pid
        else
          @logger.error "Process for #{block_path} was in the process list, but is not in the OS process list."
          @processes.delete pid
        end
      end

      def get_status
        return "lulz"
      end
    end
  end
end
