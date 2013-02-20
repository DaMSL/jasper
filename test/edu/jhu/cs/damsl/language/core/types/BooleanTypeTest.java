package edu.jhu.cs.damsl.language.core.types;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.jhu.cs.damsl.engine.storage.Tuple;

public class BooleanTypeTest {
  @Test
  public void readWriteTest() {
    BooleanType type  = new BooleanType();
    Boolean in1 = false;
    Boolean in2 = true;
    
    Tuple t = Tuple.emptyTuple(
        type.getInstanceSize(in1)+type.getInstanceSize(in2), false);
    type.writeValue(in1, t);
    type.writeValue(in2, t);
    Boolean out1 = (Boolean) type.readValue(t);
    Boolean out2 = (Boolean) type.readValue(t);
    assertTrue(out1.equals(in1) && out2.equals(in2));
  }
}
