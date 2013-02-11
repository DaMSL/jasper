package edu.jhu.cs.damsl.language.core.types;

import edu.jhu.cs.damsl.engine.storage.Tuple;

public class LongType extends OrderedType {

  public LongType() { super(Long.class); }

  // Ordered type helpers.
  @Override public boolean isBounded() { return true; }
  @Override public Object neutralValue() { return 0L; }
  @Override public Object minValue() { return Long.MIN_VALUE; }
  @Override public Object maxValue() { return Long.MAX_VALUE; }

  @Override
  public Integer getSize() { return Long.SIZE >> 3; }

  @Override
  public Integer getInstanceSize(Object o) { 
    if ( o instanceof Long ) return getSize();
    return -1;
  }

  @Override
  public Object newValue() { return Long.valueOf(0); }

  @Override
  public Object readValue(Tuple t) { return t.readLong(); }

  @Override
  public void writeValue(Object v, Tuple t) { t.writeLong((Long) v); } 

  @Override
  public Object parseType(String s) {
      return (Object)Long.valueOf(s);
  }
}
