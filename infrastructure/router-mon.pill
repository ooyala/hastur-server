# This is an intentionally 'lush' file - we don't need all these
# checks, but it makes it clear how to do them.

Bluepill.application("hastur_router") do |app|
  # Can also set per-process.  Let's not.
  #app.uid = "hastur"
  #app.gid = "hastur"

  app.process("hastur_router_1") do |process|
    process.start_command = "hastur-router"
    #process.stop_command = "kill -QUIT {{PID}}"
    process.stop_signals = [:quit, 30.seconds, :term, 5.seconds, :kill]
    #process.working_dir = ""
    #process.stdout = process.stderr = "/tmp/router.log"
    process.pid_file = "/tmp/hastur-router-1.pid"
    process.daemonize = true

    process.start_grace_time = 3.seconds
    process.stop_grace_time = 5.seconds
    process.restart_grace_time = 8.seconds

    process.checks :mem_usage, :every => 10.seconds, :below => 100.megabytes, :times => [3,5]
    process.checks :flapping, :times => 2, :within => 30.seconds, :retry_in => 15.seconds
  end
end
