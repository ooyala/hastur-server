package hastur.client;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
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
   * Records a stat.
   *
   * @param name - Unique dotted-name that describes the stat
   * @param stat - Value of the stat
   * @param unit - A unit of measurement that is associated with stat
   @ @param tags - A list of strings which describes the stat
   *
   */
  public static boolean recordStat(String name, double stat, String unit,
                                   List<String> tags) {
    JSONObject o = new JSONObject();
    try {
      o.put("name", name);
      o.put("stat", stat);
      o.put("unit", unit);
      o.put("tags", flattenList(tags));
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
    return udpSend(o);
  }

  /**
   * Records an application heartbeat. If the application does not 
   * continue to send heartbeats, it implies that something is wrong
   * with the application.
   */
  public static boolean recordHeartbeat(String service, double interval) {
    JSONObject o = new JSONObject();
    return udpSend(o);
  }

  /**
   * Registers a plugin with Hastur.
   */ 
  public static boolean registerPlugin(String path, String args, String name, double interval) {
    JSONObject o = new JSONObject();
    return udpSend(o);
  }

  /**
   * Registers the application with Hastur.
   */ 
  public static boolean registerService(String service) {
    JSONObject o = new JSONObject();
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
