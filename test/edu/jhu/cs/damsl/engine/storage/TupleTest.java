package edu.jhu.cs.damsl.engine.storage;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.Schema.Field;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.language.core.types.DoubleType;
import edu.jhu.cs.damsl.language.core.types.IntType;
import edu.jhu.cs.damsl.language.core.types.LongType;
import edu.jhu.cs.damsl.language.core.types.StringType;
import edu.jhu.cs.damsl.language.core.types.Type;
import edu.jhu.cs.damsl.utils.StreamGenerator;

public class TupleTest {
  
  private String streamName;
  private LinkedHashMap<Schema, StreamGenerator> testSchemas;

  private Schema schema;
  private List<Object> values;

  @Before
  public void setUp() throws Exception {
    streamName = "R";
    LinkedHashMap<String, Type> fields = new LinkedHashMap<String, Type>();
    fields.put("x", new IntType());
    fields.put("y", new IntType());
    fields.put("z", new IntType());
    fields.put("a", new IntType());
    schema = new Schema(streamName+"_schema",fields);
    values = new LinkedList<Object>(Arrays.asList(0, 10, 100, 1000));
    
    Field xi = new Schema("test").new Field("x", new IntType());
    Field xl = new Schema("test").new Field("x", new LongType());
    Field yi = new Schema("test").new Field("y", new IntType());
    Field zi = new Schema("test").new Field("z", new IntType());
    Field zd = new Schema("test").new Field("z", new DoubleType());
    Field zs = new Schema("test").new Field("z", new StringType());
    Schema[] schArray = new Schema[] {
        new Schema("iii", xi, yi, zi),
        new Schema("lid", xl, yi, zd),
        new Schema("lis", xl, yi, zs)
    };
    
    testSchemas = new LinkedHashMap<Schema, StreamGenerator>();
    for ( Schema s : schArray ) testSchemas.put(s, new StreamGenerator(s));
  }
  
  // Empty tuple tests
  
  @Test
  public void emptyHeapTupleTest() {
    int tupleSize = 100;
    int expectedTupleSize = tupleSize+Tuple.headerSize;
    Tuple fixedLenTuple = Tuple.emptyTuple(tupleSize, false);
    Tuple varLenTuple = Tuple.emptyTuple(tupleSize, true);
    
    //System.out.println(fixedLenTuple);

    assertTrue(fixedLenTuple.capacity() == expectedTupleSize
                && fixedLenTuple.getFixedLength() == expectedTupleSize);
    
    assertTrue(varLenTuple.capacity() == expectedTupleSize
                && varLenTuple.getFixedLength() < 0);
  }

  @Test
  public void schemaTupleTest() {
    Tuple t = Tuple.schemaTuple(schema, values);
    int expectedSize = schema.getTupleSize()+Tuple.headerSize;
    assertTrue(t.size() == expectedSize && t.capacity() == expectedSize
                && t.isFixedLength());
    
    for (Map.Entry<Schema, StreamGenerator> sv : testSchemas.entrySet()) {
      List<Object> values = sv.getValue().generateValues();
      int fixedLen = sv.getKey().getTupleSize();
      int aSize =
        (fixedLen > 0? fixedLen :
          sv.getKey().getTupleSize(values))+Tuple.headerSize;
      
      Tuple a = Tuple.schemaTuple(sv.getKey(), values);
      assertTrue(a.size() == aSize && a.capacity() == aSize
                  && a.isFixedLength() == (fixedLen>0));
    }
  }
  
  @Test
  public void getTupleTest() {
    Tuple t = Tuple.schemaTuple(schema, values);
    ChannelBuffer cb = Tuple.schemaTuple(schema, values).toBuffer();
    ChannelBuffer cb2 = Tuple.schemaTuple(schema, values).toBuffer();
    
    int schemaSize = schema.getTupleSize();
    int expectedSize = schemaSize+Tuple.headerSize;

    Tuple t2 = Tuple.getTuple(t);
    Tuple t3 = Tuple.getTuple(cb, expectedSize, cb.writerIndex());
    Tuple t4 = Tuple.getTuple(cb2, cb2.writerIndex());

    // Check that t has advanced, and that t2 is now populated.
    assertTrue(t.size() == 0 && 
        t2.size() == expectedSize && t2.capacity() == schemaSize);

    // Check that cb has advanced and that t3 is now populated.
    assertTrue(cb.readableBytes() == 0 &&
        t3.size() == expectedSize && t3.capacity() == schemaSize);

    // Check that cb2 has advanced and that t3 is now populated.
    assertTrue(cb2.readableBytes() == 0 &&
        t4.size() == expectedSize && t4.capacity() == schemaSize);
  }
  
  // Concat test
  @Test
  public void concatTest() {
    Tuple t = Tuple.schemaTuple(schema, values);
    Tuple t2 = Tuple.schemaTuple(schema, values);
    
    Tuple t3 = t.concatTuple(t2);
    
    assertTrue(t3.size() == t.size()+t2.size()
                && t3.isFixedLength() && t3.isMultiTuple() && t3.count() == 2);
  }

  // N-tuple constructor test
  @Test
  public void getNTupleTest() {
    int tupleSize = schema.getTupleSize()+Tuple.headerSize;
    int numTuples = 10;
    Tuple t = Tuple.schemaTuple(schema, values);
    Tuple t2 = Tuple.schemaTuple(schema, values);
    for (int i = 1; i < numTuples; ++i) {
      t = t.concatTuple(Tuple.schemaTuple(schema, values));
      t2 = t2.concatTuple(Tuple.schemaTuple(schema, values));
    }

    int count = 5;
    Tuple t3 = Tuple.getNTuples(t, count, t.writerIndex());
    Tuple t4 = Tuple.getNTuples(t2, t2.getFixedLength(), count, t.writerIndex());

    assertTrue(t3.isMultiTuple() && !t3.isFixedLength()
                && t3.count() == count && t3.size() == count*tupleSize);
    
    assertTrue(t4.isMultiTuple() && t4.isFixedLength()
                && t4.count() == count && t4.size() == count*tupleSize);
  }

  // TODO:
  // -- Multi-tuple traversal tests
  // -- Interpret test
}
