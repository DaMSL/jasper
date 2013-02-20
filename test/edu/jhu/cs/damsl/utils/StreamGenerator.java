package edu.jhu.cs.damsl.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.language.core.types.BooleanType;
import edu.jhu.cs.damsl.language.core.types.DoubleType;
import edu.jhu.cs.damsl.language.core.types.FloatType;
import edu.jhu.cs.damsl.language.core.types.IntType;
import edu.jhu.cs.damsl.language.core.types.LongType;
import edu.jhu.cs.damsl.language.core.types.Type;

public class StreamGenerator {

  private String[] dictionary = new String[] {
      "the", "cat", "sat", "on", "mat",
      "quick", "brown", "fox", "jumps", "over", "lazy", "dog"
    };

  Schema schema;

  public StreamGenerator(Schema s) { schema = s; }

  public List<Object> generateValues() {
    List<Object> r = new LinkedList<Object>();
    Random rng = new Random();
    for ( Type t : schema.getTypes() ) {
      if ( t instanceof IntType ) {
        r.add(rng.nextInt());
      } else if ( t instanceof LongType ) {
        r.add(rng.nextLong());
      } else if ( t instanceof FloatType ) {
        r.add(rng.nextFloat());
      } else if ( t instanceof DoubleType ) {
        r.add(rng.nextDouble());
      } else if ( t instanceof BooleanType ) {
        r.add(rng.nextBoolean());
      } else {
        // String type
        StringBuffer str = new StringBuffer();
        int numWords = rng.nextInt(9)+1;
        for (int i = 0; i < numWords; ++i) {
          if ( str.length() > 0 ) str.append(" ");
          str.append(dictionary[rng.nextInt(dictionary.length)]);
        }
        r.add(str.toString());
      }
    }
    return r;
  }

  // Returns a heap-allocated stream.
  public List<Tuple> generate(int n) {
    List<Tuple> r = new LinkedList<Tuple>();
    for (int i = 0; i < n; ++i)
      r.add(Tuple.schemaTuple(schema, generateValues()));
    return r;
  }
}
