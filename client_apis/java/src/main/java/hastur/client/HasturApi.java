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
  private static HeartbeatThread heartbeatThread;

  private static final long SECS_2100       = 4102444800L;
  private static final long MILLI_SECS_2100 = 4102444800000L;
  private static final long MICRO_SECS_2100 = 4102444800000000L;
  private static final long NANO_SECS_2100  = 4102444800000000000L;

  private static final long SECS_1971       = 31536000L;
  private static final long MILLI_SECS_1971 = 31536000000L;
  private static final long MICRO_SECS_1971 = 31536000000000L;
  private static final long NANO_SECS_1971  = 31536000000000000L;

  public static final int HASTUR_API_HEARTBEAT_INTERVAL = 10;

  // Automatically send heartbeats whenever this library is loaded in the CL
  static {
    try {
      heartbeatThread = HeartbeatThread.getInstance();
      heartbeatThread.setIntervalSeconds((double)HASTUR_API_HEARTBEAT_INTERVAL);
      heartbeatThread.start();
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Computes if a number is inclusively between a range.
   */
  public static boolean isBetween(long x, long lower, long upper) {
    return lower <= x && x <= upper;
  }

  /**
   * Returns the timestamp in terms of microseconds since 1971. Guesses
   * based on the ranges.
   */
  protected static long normalizeTimestamp(long time) {
    if(isBetween(time, SECS_1971, SECS_2100)) {
      return time * 1000000;
    } else if(isBetween(time, MILLI_SECS_1971, MILLI_SECS_2100)) {
      return time * 1000;
    } else if(isBetween(time, MICRO_SECS_1971, MICRO_SECS_2100)) {
      return time;
    } else if(isBetween(time, NANO_SECS_1971, NANO_SECS_2100)) {
      return time / 1000;
    } else {
      throw new IllegalArgumentException("Unable to validate timestamp: " + time);
    }
  }

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
      o.put("timestamp", normalizeTimestamp(timestamp));
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
      o.put("timestamp", normalizeTimestamp(timestamp));
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
      o.put("timestamp", normalizeTimestamp(timestamp));
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
   * Constructs and sends heartbeat UDP packets. Interval is given in seconds.
   */
  protected static boolean heartbeat(String heartbeatName, String appName) {
    JSONObject o = new JSONObject();
    try {
      o.put("_route", "heartbeat");
      o.put("app", appName);
      o.put("name", heartbeatName);
    } catch(Exception e) {
      e.printStackTrace();
      return false;
    }
    return udpSend(o);
  }

  /**
   * Constructs and sends heartbeat UDP packets. Interval is given in seconds.
   * The application name will be automatically computed if possible.  
   */
  public static boolean heartbeat(String heartbeatName) {
    return heartbeat(heartbeatName, computeAppName());
  }

  /**
   * Dynamically compute the application name
   */
  private static String computeAppName() {
    StackTraceElement[] stack = Thread.currentThread ().getStackTrace ();
    StackTraceElement main = stack[stack.length - 1];
    return main.getClassName();
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
