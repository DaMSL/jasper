package edu.jhu.cs.damsl.catalog;

import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import edu.jhu.cs.damsl.language.core.types.StringType;
import edu.jhu.cs.damsl.language.core.types.Type;
import edu.jhu.cs.damsl.language.core.types.Typing;

public class SchemaTest {
  private LinkedHashMap<String, Type> lid1Fields;
  private LinkedHashMap<String, Type> ls1Fields;
  private LinkedHashMap<String, Type> ls2Fields;
  private LinkedHashMap<String, Type> ls3Fields;
  private Schema lid1;
  private Schema ls1;
  private Schema ls2;
  private Schema ls3;
  
  @Before
  public void setUp() throws ClassNotFoundException {
    lid1Fields = new LinkedHashMap<String, Type>();
    lid1Fields.put("x", Typing.getType("long"));
    lid1Fields.put("y", Typing.getType("int"));
    lid1Fields.put("z", Typing.getType("double"));
    lid1 = new Schema("lid1", lid1Fields);
    
    ls1Fields = new LinkedHashMap<String, Type>();
    ls1Fields.put("x", Typing.getType("long"));
    ls1Fields.put("y", Typing.getType("string"));
    ls1 = new Schema("ls1", ls1Fields);

    ls2Fields = new LinkedHashMap<String, Type>();
    ls2Fields.put("a", Typing.getType("long"));
    ls2Fields.put("b", Typing.getType("string"));
    ls2 = new Schema("ls2", ls2Fields);
    
    ls3Fields = new LinkedHashMap<String, Type>();
    ls3Fields.put("x", Typing.getType("long"));
    ls3Fields.put("y", Typing.getType("string"));
    ls3 = new Schema("ls3", ls3Fields);
  }

  @Test
  public void testHasField() {
    assertTrue(ls1.hasField("y") && !ls1.hasField("z"));
  }

  @Test
  public void testFieldPosition() {
    assertTrue(ls1.getFieldPosition("y") == 1
                && ls1.getFieldPosition("z") == null);
  }

  public void testGetTupleSize() {
    String s = new String("hello world");
    List<Object> fields = new LinkedList<Object>();
    fields.add(10L);
    fields.add(s);
    int expectedSize = (Long.SIZE >> 3) +
      (Integer.SIZE >> 3) + StringType.getStringByteLength(s);
    
    assertTrue(lid1.getTupleSize() == 20 && ls1.getTupleSize() < 0
                  && ls1.getTupleSize(fields) == expectedSize);
  }
  
  @Test
  public void testMapEquals() {
    assertTrue(ls1Fields.equals(ls3Fields));
  }

  @Test
  public void testNamedMatch() {
    assertTrue(ls1.namedMatch(ls3));
    assertTrue(ls3.namedMatch(ls1));
  }

  @Test
  public void testUnnamedMatch() {
    assertTrue(ls1.unnamedMatch(ls2));
    assertTrue(ls2.unnamedMatch(ls1));
  }

}
