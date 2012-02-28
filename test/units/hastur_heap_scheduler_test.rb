$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "infrastructure")

require "test/unit"

require "hastur-heap-scheduler"

class HasturHeapSchedulerTest < Test::Unit::TestCase 
  def test_job
    # TODO(viet): Write the actual tests. Probably need to add a "test mode" to hastur-heap-scheduler
    #             to collect the order/timing of the jobs.
  end
end
