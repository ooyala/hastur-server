package hastur.client;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;

/**
 * A set of APIs to allow the interaction between a service and the Hastur client daemon. The
 * assumption is that all services are running on the same machine as the daemon.
 */
public class HasturApi {

  private static int udpPort = 8125;
  private static InetAddress localAddr;

  /**
   * Sends a UDP packet to 127.0.0.1:8125. 
   */ 
  protected static boolean udpSend(JSONObject json) {
    DatagramSocket socket = null;
    boolean success = true;
    try {
      socket = new DatagramSocket();
      String msgString = json.toString();
      DatagramPacket msg = new DatagramPacket(msgString.getBytes(), 
                                              msgString.length(), 
                                              getLocalAddr(), 
                                              getUdpPort());
      socket.send(msg);
    } catch(Exception e) {
      e.printStackTrace();
      success = false;
    } finally {
      if(socket != null) {
        socket.close();
      }
    }
    return success;
  }

  /**
   * Flattens a list of strings into a space-separated String.
   */
  private static String flattenList(List<String> l) {
    if(l == null) l = new ArrayList<String>();
    StringBuilder sBuilder = new StringBuilder();
    for(String s : l) { sBuilder.append(s); sBuilder.append(" "); }
    return sBuilder.toString();
  }

  /**
   * Generates a string representation of the labels in JSON format.
   */
  protected static String generateLabelsJson(Map<String, String> labels) throws org.json.JSONException {
    JSONObject o = new JSONObject();
    if(labels != null) {
      for(String key : labels.keySet()) {
        o.put(key, labels.get(key));
      }
    }
    return o.toString();
  }

  /**
   * Sends a 'mark' stat to Hastur client daemon.
   */
  public static boolean mark(String name, Long timestamp, Map<String, String> labels) {
    if(timestamp == null) timestamp = System.nanoTime() / 1000;
    JSONObject o = new JSONObject();
    try {
      o.put("_route", "stat");
      o.put("type", "mark");
      o.put("name", name);
      o.put("timestamp", timestamp);
      o.put("labels", generateLabelsJson(labels));
    } catch(Exception e) {
      e.printStackTrace();
      return false;
    }
    return udpSend(o);
  }

  /**
   * Sends a 'counter' stat to Hastur client daemon.
   */
  public static boolean counter(String name, Long timestamp, double increment, Map<String, String> labels) {
    if(timestamp == null) timestamp = System.nanoTime() / 1000;
    JSONObject o = new JSONObject();
    try {
      o.put("_route", "stat");
      o.put("type", "counter");
      o.put("name", name);
      o.put("timestamp", timestamp);
      o.put("increment", increment);
      o.put("labels", generateLabelsJson(labels));
    } catch(Exception e) {
      e.printStackTrace();
      return false;
    }
    return udpSend(o);
  }

  /**
   * Sends a 'gauge' stat to Hastur client daemon.
   */
  public static boolean gauge(String name, Long timestamp, double value, Map<String, String> labels) {
    if(timestamp == null) timestamp = System.nanoTime() / 1000;
    JSONObject o = new JSONObject();
    try {
      o.put("_route", "stat");
      o.put("type", "gauge");
      o.put("name", name);
      o.put("timestamp", timestamp);
      o.put("value", value);
      o.put("labels", generateLabelsJson(labels));
    } catch(Exception e) {
      e.printStackTrace();
      return false;
    }
    return udpSend(o);
  }

  /**
   * Notifies the Hastur client of a problem in the application.
   */
  public static boolean notify(String message) {
    JSONObject o = new JSONObject();
    try {
      o.put("_route", "notification");
      o.put("message", message);
    } catch(Exception e) {
      e.printStackTrace();
      return false;
    }
    return udpSend(o);
  }

  /**
   * Registers a plugin with Hastur.
   */ 
  public static boolean registerPlugin(String path, String args, String name, double interval) {
    JSONObject o = new JSONObject();
    try {
      o.put("_route", "register_plugin");
      o.put("plugin_path", path);
      o.put("plugin_args", args);
      o.put("interval", interval);
      o.put("plugin", name);
    } catch(Exception e) {
      e.printStackTrace();
      return false;
    }
    return udpSend(o);
  }

  /**
   * Registers the application with Hastur.
   */ 
  public static boolean registerService(String service) {
    JSONObject o = new JSONObject();
    try {
      o.put("_route", "register_service");
      o.put("app", service);
    } catch(Exception e) {
      e.printStackTrace();
      return false;
    }
    return udpSend(o);
  }

  /**
   * Retrieves the localhost InetAddress object.
   */
  private static InetAddress getLocalAddr() {
    if(localAddr == null) {
      try {
        localAddr = InetAddress.getByName("127.0.0.1");
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }
    return localAddr;
  }

  /**
   * Returns the UDP port that this library sends messages to.
   */
  public static int getUdpPort() {
    return udpPort;
  }

  /**
   * Sets the UDP port that this library sends messages to.
   */ 
  public static void setUdpPort(int port) {
    udpPort = port;
  }
}
