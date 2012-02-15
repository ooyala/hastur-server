package hastur.client;

import org.json.JSONObject;

/**
 * Heartbeat thread that periodically sends an application heartbeat. Only one
 * should HeartbeatThread should exist per application.
 */
public class HeartbeatThread extends Thread {

  private static final HeartbeatThread instance = new HeartbeatThread();

  private Double intervalSeconds;
  private JSONObject o;

  private HeartbeatThread() {

  }

  public static HeartbeatThread getInstance() {
    return instance;
  }

  public void setIntervalSeconds(Double intervalSeconds) {
    this.intervalSeconds = intervalSeconds;
  }

  public void setJson(JSONObject o) throws org.json.JSONException {
    this.o = o;
  }

  public void run() {
    while(true) {
      try {
        HasturApi.udpSend(o);
        Thread.sleep((int)(intervalSeconds * 1000));
      } catch(java.lang.InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
