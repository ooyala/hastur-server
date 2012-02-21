package hastur.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

/**
 * Starts a background thread that will execute blocks of code every so often.
 */
public class HasturClientThread extends Thread {

  // makes this class a Singleton
  private static final HasturClientThread instance = new HasturClientThread();
  // holds all of the scheduledJobs, broken down by time intervals
  private static Map<HasturTime, List<HasturJob>> scheduledJobs;
  // holds all of the elapsed time for each interval. 
  private static Map<HasturTime, Long> lastTime;
  
  public static final String CLIENT_HEARTBEAT = "client_heartbeat";

  /**
   * Set up the background thread for scheduling.
   * Must keep this constructor private to ensure the characteristics of a singleton.
   */
  private HasturClientThread() {
    // sets up the scheduledJobs map
    scheduledJobs = new HashMap<HasturTime, List<HasturJob>>();
    lastTime = new HashMap<HasturTime, Long>();
    for(HasturTime t : HasturTime.values()) {
      scheduledJobs.put(t, new ArrayList<HasturJob>());
      lastTime.put(t, 0L);
    }
  }

  /**
   * Retrieves the singleton instance.
   */
  public static HasturClientThread getInstance() {
    return instance;
  }

  /**
   * Schedules a job for a given HasturTime.
   */
  public static boolean addJob(HasturJob job) {
    synchronized(scheduledJobs) {
      return scheduledJobs.get(job.getInterval()).add(job);
    }
  }
  
  /**
   * Continuously updates and executes jobs.
   */
  public void run() {
    int waitIntervals[] = { 5000, 1000*60, 1000*60*60, 1000*60*60*24 };
    while(true) {
      int idx = 0;
      try {
        // for each of the interval buckets
        for(HasturTime t : HasturTime.values()) {
          long currTime = System.currentTimeMillis();
          // execute the scheduled items if time is up
          if(currTime - lastTime.get(t) >= waitIntervals[idx]) {
            // update the time
            lastTime.put(t, currTime);    
            // execute all of the jobs scheduled to run during this time frame
            for(HasturJob job : scheduledJobs.get(t)) {
              job.call();
            }
          }
          idx++;
        }
        Thread.sleep(1000);             // rest
      } catch(java.lang.InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
