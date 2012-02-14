package hastur.client;

import hastur.client.HasturApi;

/**
 * Only do .* for tests. Do not do this in shipped code.
 */
import org.junit.*;
import static org.junit.Assert.*;
import java.net.*;
import org.json.JSONObject;

public class HasturApiTest {

  private DatagramSocket server = null;

  @Before
  public void setUp() {
    try {
      server = new DatagramSocket(HasturApi.getUdpPort());
      server.setSoTimeout(1000);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() {
    if(server != null) {
      server.close();
    }
  }

  @Test
  public void testGauge() {
    long currTime = System.nanoTime() / 1000;
    boolean isSuccess = HasturApi.gauge("myLatency", currTime, 9.2, null);
    try {
      DatagramPacket msg = new DatagramPacket(new byte[65000], 65000);
      server.receive(msg);
      String rawMsg = new String(msg.getData());
      JSONObject o = new JSONObject(rawMsg);
      assertEquals("myLatency", o.get("name"));
      assertEquals("stat", o.get("_route"));
      assertEquals("gauge", o.get("type"));
      assertEquals(currTime, o.get("timestamp"));
      assertEquals(9.2, o.get("value"));
      assertEquals("{}", o.get("labels"));
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
    assertTrue(isSuccess);
  }


  @Test
  public void testCounter() {
    long currTime = System.nanoTime() / 1000; 
    boolean isSuccess = HasturApi.counter("myLatency", currTime, 2, null);
    try {
      DatagramPacket msg = new DatagramPacket(new byte[65000], 65000);
      server.receive(msg);
      String rawMsg = new String(msg.getData());
      JSONObject o = new JSONObject(rawMsg);
      assertEquals("myLatency", o.get("name"));
      assertEquals("stat", o.get("_route"));
      assertEquals(currTime, o.get("timestamp"));
      assertEquals(2, o.get("increment"));
      assertEquals("counter", o.get("type"));
      assertEquals("{}", o.get("labels"));
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
    assertTrue(isSuccess);
  }


  @Test
  public void testMark() {
    long currTime = System.currentTimeMillis();
    boolean isSuccess = HasturApi.mark("myLatency", currTime, null);
    try {
      DatagramPacket msg = new DatagramPacket(new byte[65000], 65000);
      server.receive(msg);
      String rawMsg = new String(msg.getData());
      JSONObject o = new JSONObject(rawMsg);
      assertEquals("myLatency", o.get("name"));
      assertEquals("stat", o.get("_route"));
      assertEquals("mark", o.get("type"));
      assertEquals("{}", o.get("labels"));
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
    assertTrue(isSuccess);
  }

  @Test
  public void testHeartbeat() {
    HasturApi.heartbeat("myApp", 2);  // heartbeat every 2 seconds
    try {
      Thread.sleep(2500);
      DatagramPacket msg = new DatagramPacket(new byte[65000], 65000);
      int numMsgs = 0;
      for(int i = 0 ; i < 2; i++) {
        server.receive(msg);
        numMsgs++;
      }
      assertEquals(2, numMsgs);
      String rawMsg = new String(msg.getData());
      JSONObject o = new JSONObject(rawMsg);
      assertEquals("heartbeat", o.get("_route"));
      assertEquals("myApp", o.get("app"));
      assertEquals(2, o.get("interval"));

    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
}
