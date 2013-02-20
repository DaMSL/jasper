package edu.jhu.cs.damsl.utils;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.Schema.Field;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.language.core.types.DoubleType;
import edu.jhu.cs.damsl.language.core.types.IntType;
import edu.jhu.cs.damsl.language.core.types.LongType;
import edu.jhu.cs.damsl.language.core.types.StringType;

public class StandardGenerator {
  private LinkedHashMap<Schema, StreamGenerator> testSchemas;

  public StandardGenerator() {
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
  
  public List<Tuple> generateData() {
    LinkedList<Tuple> r = new LinkedList<Tuple>();
    for (Map.Entry<Schema, StreamGenerator> sv : testSchemas.entrySet()) {
      List<Object> values = sv.getValue().generateValues();
      r.add(Tuple.schemaTuple(sv.getKey(), values));
    }
    return r;
  }
  
  public List<Tuple> generateData(Schema s, int count) {
    StreamGenerator gen = new StreamGenerator(s);
    LinkedList<Tuple> r = new LinkedList<Tuple>();
    for (int i = count; i > 0; --i) {
      List<Object> values = gen.generateValues();
      r.add(Tuple.schemaTuple(s, values));
    }
    return r;
  }
  
  public LinkedHashMap<Schema, Tuple> generateTypedData() {
    LinkedHashMap<Schema, Tuple> r = new LinkedHashMap<Schema, Tuple>();
    for (Map.Entry<Schema, StreamGenerator> sv : testSchemas.entrySet()) {
      List<Object> values = sv.getValue().generateValues();
      r.put(sv.getKey(), Tuple.schemaTuple(sv.getKey(), values));
    }
    return r;
  }
  
  public Set<Schema> getSchemas() { return testSchemas.keySet(); }

}
