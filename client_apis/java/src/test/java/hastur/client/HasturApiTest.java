package hastur.client;

import hastur.client.*;

/**
 * Only do .* for tests. Do not do this in shipped code.
 */
import org.junit.*;
import static org.junit.Assert.*;
import java.net.*;
import java.util.*;
import org.json.JSONObject;

public class HasturApiTest {

  private DatagramSocket server = null;

  @Before
  public void setUp() {
    HasturApi.__isTestMode(true);
  }

  @After
  public void tearDown() {
    HasturApi.__clearBufferedMsgs();
  }

  @Test
  public void testGauge() {
    long currTime = System.nanoTime() / 1000;
    boolean isSuccess = HasturApi.gauge("myLatency", currTime, 9.2, null);
    boolean received = false;
    List<JSONObject> msgs = HasturApi.__getBufferedMsgs();
    try {
      JSONObject o = msgs.get(msgs.size()-1);
      if(o.get("_route").equals("stat")) {
        assertEquals("myLatency", o.get("name"));
        assertEquals("stat", o.get("_route"));
        assertEquals("gauge", o.get("type"));
        assertEquals(currTime, o.get("timestamp"));
        assertEquals(9.2, o.get("value"));
        assertNotNull(o.get("labels"));
        assertTrue(o.has("labels"));
        assertTrue(((JSONObject)o.get("labels")).has("app"));
        assertTrue(((JSONObject)o.get("labels")).has("pid"));
        assertTrue(((JSONObject)o.get("labels")).has("tid"));
        received = true;
      }
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
    assertTrue(received);
    assertTrue(isSuccess);
  }

  @Test
  public void testCounter() {
    long currTime = System.nanoTime() / 1000; 
    boolean isSuccess = HasturApi.counter("myLatency", currTime, 2.0, null);
    List<JSONObject> msgs = HasturApi.__getBufferedMsgs();
    try {
      JSONObject o = msgs.get(msgs.size()-1);
      assertEquals("myLatency", o.get("name"));
      assertEquals("stat", o.get("_route"));
      assertEquals(currTime, o.get("timestamp"));
      assertEquals(2d, o.get("increment"));
      assertEquals("counter", o.get("type"));
      assertNotNull(o.get("labels"));
      assertTrue(o.has("labels"));
      assertTrue(((JSONObject)o.get("labels")).has("app"));
      assertTrue(((JSONObject)o.get("labels")).has("pid"));
      assertTrue(((JSONObject)o.get("labels")).has("tid"));
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
    List<JSONObject> msgs = HasturApi.__getBufferedMsgs();
    try {
      JSONObject o = msgs.get(msgs.size()-1);
      assertEquals("myLatency", o.get("name"));
      assertEquals("stat", o.get("_route"));
      assertEquals("mark", o.get("type"));
      assertTrue(o.has("labels"));
      assertTrue(((JSONObject)o.get("labels")).has("app"));
      assertTrue(((JSONObject)o.get("labels")).has("pid"));
      assertTrue(((JSONObject)o.get("labels")).has("tid"));
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
    assertTrue(isSuccess);
  }

  @Test
  public void testApiHeartbeat() {
    try {
      Thread.sleep(60*1000);    // 60 seconds
      List<JSONObject> msgs = HasturApi.__getBufferedMsgs();
      assertTrue(msgs.size() > 0);
      JSONObject o = msgs.get(msgs.size()-1);
      assertEquals("heartbeat", o.get("_route"));
      assertTrue(((JSONObject)o.get("labels")).has("pid"));
      assertTrue(((JSONObject)o.get("labels")).has("tid"));
      assertTrue(((JSONObject)o.get("labels")).has("app"));
      assertEquals(HasturClientThread.CLIENT_HEARTBEAT, ((JSONObject)o.get("labels")).get("app"));
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

  @Test
  public void testHeartbeat() {
    String appName = "foobar";
    Map<String, String> labels = new HashMap<String, String>();
    labels.put("app", appName);
    HasturApi.heartbeat(labels);
    List<JSONObject> msgs = HasturApi.__getBufferedMsgs();
    try {
      JSONObject o = msgs.get(msgs.size()-1);
      assertEquals("heartbeat", o.get("_route"));
      assertTrue(o.has("labels"));
      assertTrue(((JSONObject)o.get("labels")).has("app"));
      assertEquals(appName, ((JSONObject)o.get("labels")).get("app"));
      assertTrue(((JSONObject)o.get("labels")).has("pid"));
      assertTrue(((JSONObject)o.get("labels")).has("tid"));
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
  
  @Test
  public void testNotify() {
    String message = "This is my message.";
    HasturApi.notify(message, null);
    List<JSONObject> msgs = HasturApi.__getBufferedMsgs();
    try {
      JSONObject o = msgs.get(msgs.size()-1);
      assertEquals("notification", o.get("_route"));
      assertEquals(message, o.get("message"));
      assertTrue(o.has("labels"));
      assertTrue(((JSONObject)o.get("labels")).has("app"));
      assertTrue(((JSONObject)o.get("labels")).has("pid"));
      assertTrue(((JSONObject)o.get("labels")).has("tid"));
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }   
  }

  @Test
  public void testRegisterPlugin() {
    String path = "myPath";
    String args = "myArgs";
    String name = "myName";
    double interval = 5.5;
    HasturApi.registerPlugin(path, args, name, interval, null);
    List<JSONObject> msgs = HasturApi.__getBufferedMsgs();
    try {
      JSONObject o = msgs.get(msgs.size()-1);
      assertEquals("register_plugin", o.get("_route"));
      assertEquals(path, o.get("plugin_path"));
      assertEquals(args, o.get("plugin_args"));
      assertEquals(name, o.get("plugin"));
      assertEquals(interval, (Double)o.get("interval"));
      assertTrue(o.has("labels"));
      assertTrue(((JSONObject)o.get("labels")).has("app"));
      assertTrue(((JSONObject)o.get("labels")).has("pid"));
      assertTrue(((JSONObject)o.get("labels")).has("tid"));
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }  
  }

  @Test
  public void testRegisterService() {
    Map<String, String> labels = new HashMap<String, String>();
    labels.put("foo", "bar");
    HasturApi.registerService(labels);
    List<JSONObject> msgs = HasturApi.__getBufferedMsgs();
    try {
      JSONObject o = msgs.get(msgs.size()-1);
      assertEquals("register_service", o.get("_route"));
      assertTrue(o.has("labels"));
      assertTrue(((JSONObject)o.get("labels")).has("app"));
      assertTrue(((JSONObject)o.get("labels")).has("pid"));
      assertTrue(((JSONObject)o.get("labels")).has("tid"));
      assertTrue(((JSONObject)o.get("labels")).has("foo"));
      assertEquals("bar", (String)((JSONObject)o.get("labels")).get("foo"));
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }  
  }

  @Test
  public void testScheduleStat() {
    final String s = "test_schedule_stat";
    HasturApi.scheduleStat( new HasturJob( HasturTime.FIVE_SECS ) {
      public void call() {
        HasturApi.mark(s, System.currentTimeMillis(), null);
      }
    });
    try {
      Thread.sleep(5000);
      List<JSONObject> msgs = HasturApi.__getBufferedMsgs();
      JSONObject o = msgs.get(msgs.size()-1);
      assertEquals("stat", o.get("_route"));
      assertEquals("mark", o.get("type"));
      assertTrue(o.has("labels"));
      assertEquals(s, o.get("name"));
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

}
