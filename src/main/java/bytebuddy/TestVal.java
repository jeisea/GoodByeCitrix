package bytebuddy;

public class TestVal {
  public Long val;

  public TestVal(Long val) {
    this.val = val;
  }

  public Long getVal() {
    return val;
  }

  public String toString() {
    return "My value is: " + val;
  }
}
