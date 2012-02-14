package hastur.client;

import org.json.JSONObject;

/**
 * Heartbeat thread that periodically sends an application heartbeat. Only one
 * should HeartbeatThread should exist per application.
 */
public class HeartbeatThread extends Thread {

  private Double intervalSeconds;
  private JSONObject o;

  public HeartbeatThread(JSONObject o) throws org.json.JSONException {
    intervalSeconds = (Double)o.get("interval");
    this.o = o;
  }

  public void setIntervalSeconds(Double intervalSeconds) {
    this.intervalSeconds = intervalSeconds;
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
