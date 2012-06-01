require_relative "../../test_helper"

require "minitest/autorun"
require "hastur-server/hastur-heap-scheduler"

class HasturHeapSchedulerTest < MiniTest::Unit::TestCase

  def test_job
    job1 = '{ "type":"plugin", "plugin_path":"echo", "plugin_args":"A OK", "interval":"five_minutes", "plugin":"myPlugin", "uuid":"84f5aea0-449b-012f-e937-109addba6b5d", "labels": {} }'
    job2 = '{ "type":"plugin", "plugin_path":"echo", "plugin_args":"OK", "interval":"five_minutes", "plugin":"myPlugin", "uuid":"84f5aea0-449b-012f-e937-109addba6b5d", "labels": {} }'

    uuid = "thisismyfakeuuid"

    scheduler = Hastur::Scheduler.new(nil, true)
    scheduler.start

    # schedule the jobs
    curr_time = Time.now
    jobs = [::Hastur::Job.new(job1, curr_time, uuid), ::Hastur::Job.new(job2, curr_time, uuid)]
    scheduler.add_jobs jobs

    # wait for the jobs to execute
    sleep 60*8 + 1
    
    # stop scheduling jobs
    scheduler.stop

    # get a list of all scheduled jobs
    msgs = scheduler.msg_buffer

    assert_equal 4, msgs.size

    assert_equal "A OK", MultiJson.load(msgs[0])["plugin_args"]
    assert_equal "OK", MultiJson.load(msgs[1])["plugin_args"]
    assert_equal "A OK", MultiJson.load(msgs[2])["plugin_args"]
    assert_equal "OK", MultiJson.load(msgs[3])["plugin_args"]

  end
end
