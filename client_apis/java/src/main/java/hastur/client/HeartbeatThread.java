package hastur.client;

import org.json.JSONObject;

/**
 * Heartbeat thread that periodically sends an application heartbeat. Only one
 * should HeartbeatThread should exist per application.
 */
public class HeartbeatThread extends Thread {

  private static final HeartbeatThread instance = new HeartbeatThread();
  private Double intervalSeconds;
  
  public static final String CLIENT_HEARTBEAT = "client_heartbeat";

  private HeartbeatThread() {

  }

  public static HeartbeatThread getInstance() {
    return instance;
  }

  public void setIntervalSeconds(Double intervalSeconds) {
    this.intervalSeconds = intervalSeconds;
  }

  public void run() {
    while(true) {
      try {
        HasturApi.heartbeat(CLIENT_HEARTBEAT, null);
        Thread.sleep((int)(intervalSeconds * 1000));
      } catch(java.lang.InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
