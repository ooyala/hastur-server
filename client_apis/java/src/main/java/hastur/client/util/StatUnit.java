package hastur.client.util;

public enum StatUnit {
  SECS("seconds"),
  MILLI_SECS("milli_seconds"),
  MINUTES("minutes"),
  HOURS("hours");

  private String value;

  StatUnit(String s) {
    this.value = s;
  }

  public String toString() {
    return value;
  }

  public static StatUnit fromString(String s) {
    if(s != null) {
      for(StatUnit unit : StatUnit.values()) {
        if(s.equalsIgnoreCase(unit.value)) {
          return unit;
        }
      }
    }
    return null;
  }
}
