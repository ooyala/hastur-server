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

  public static final int HASTUR_UDP_PORT = 8125;
  private static InetAddress localAddr;

  /**
   * Sends a UDP packet to 127.0.0.1:8125. 
   */ 
  protected static boolean udp_send(JSONObject json) {
    DatagramSocket socket = null;
    boolean success = true;
    try {
      socket = new DatagramSocket();
      String msgString = json.toString();
      DatagramPacket msg = new DatagramPacket(msgString.getBytes(), 
                                              msgString.length(), 
                                              getLocalAddr(), 
                                              HASTUR_UDP_PORT);
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
   */
  public static boolean recordStat(String type, String name, double stat, String unit, List<String> tags) {
    JSONObject o = new JSONObject();
    try {
      o.put("type", type);
      o.put("name", name);
      o.put("stat", stat);
      o.put("unit", unit);
      o.put("tags", flattenList(tags));
    } catch(Exception e) {
      e.printStackTrace();
      return false;
    }
    return udp_send(o);
  }

  /**
   * Notifies the Hastur client of a problem in the application.
   */
  public static boolean notify(String message) {
    JSONObject o = new JSONObject();
    return udp_send(o);
  }

  /**
   * Records an application heartbeat. If the application does not 
   * continue to send heartbeats, it implies that something is wrong
   * with the application.
   */
  public static boolean record_heartbeat(String service, double interval) {
    JSONObject o = new JSONObject();
    return udp_send(o);
  }

  /**
   * Registers a plugin with Hastur.
   */ 
  public static boolean register_plugin(String path, String args, String name, double interval) {
    JSONObject o = new JSONObject();
    return udp_send(o);
  }

  /**
   * Registers the application with Hastur.
   */ 
  public static boolean register_service(String service) {
    JSONObject o = new JSONObject();
    return udp_send(o);
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
}
