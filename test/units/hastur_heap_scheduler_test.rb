$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "infrastructure")

require "test/unit"

require "hastur-heap-scheduler"

class HasturHeapSchedulerTest < Test::Unit::TestCase 

  def test_job
    job1 = '{ "type":"plugin", "plugin_path":"echo", "plugin_args":"A OK", "interval":0.2, "plugin":"myPlugin", "uuid":"84f5aea0-449b-012f-e937-109addba6b5d", "labels": {} }'
    job2 = '{ "type":"plugin", "plugin_path":"echo", "plugin_args":"OK", "interval":1, "plugin":"myPlugin", "uuid":"84f5aea0-449b-012f-e937-109addba6b5d", "labels": {} }'

    scheduler = Hastur::Scheduler.new(nil, true)
    scheduler.start

    # schedule the jobs
    curr_time = Time.now
    jobs = [::Hastur::Job.new(job1, curr_time), ::Hastur::Job.new(job2, curr_time)]
    scheduler.add_jobs jobs

    # wait for the jobs to execute
    sleep 1.45
    
    # stop scheduling jobs
    scheduler.stop

    # get a list of all scheduled jobs
    msgs = scheduler.msg_buffer

    assert_equal 10, msgs.size
    assert_equal "OK", MultiJson.decode(msgs[1])["plugin_args"]
    assert_equal "OK", MultiJson.decode(msgs[6])["plugin_args"]
    assert_equal "A OK", MultiJson.decode(msgs[0])["plugin_args"]

  end
end
