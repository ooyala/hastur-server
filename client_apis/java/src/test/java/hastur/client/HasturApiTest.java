package hastur.client;

import hastur.client.HasturApi;
import hastur.client.util.StatUnit;

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
  public void testRecordStat() {
    boolean isSuccess = HasturApi.recordStat("myLatency", 1.2, StatUnit.SECS, null);
    try {
      DatagramPacket msg = new DatagramPacket(new byte[65000], 65000);
      server.receive(msg);
      String rawMsg = new String(msg.getData());
      JSONObject o = new JSONObject(rawMsg);
      assertEquals("myLatency", o.get("name"));
      assertEquals(1.2, o.get("stat"));
      assertEquals(StatUnit.SECS.toString(), o.get("unit"));
      assertEquals("", o.get("tags"));
    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
    assertTrue(isSuccess);
  }
}
