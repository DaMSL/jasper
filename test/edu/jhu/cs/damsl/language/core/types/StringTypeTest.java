package edu.jhu.cs.damsl.language.core.types;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.jhu.cs.damsl.engine.storage.Tuple;

public class StringTypeTest {

  @Test
  public void readWriteTest() {
    StringType type  = new StringType();
    String in = "this is a test string";
    Tuple t = Tuple.emptyTuple(type.getInstanceSize(in), true);
    type.writeValue(in, t);
    String out = (String) type.readValue(t);
    assertTrue(out.equals(in));
  }
  
  // TODO: more complex character tests.
}
