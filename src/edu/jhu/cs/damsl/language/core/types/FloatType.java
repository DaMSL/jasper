package edu.jhu.cs.damsl.language.core.types;

import edu.jhu.cs.damsl.engine.storage.Tuple;

public class FloatType extends OrderedType {

  public FloatType() { super(Float.class); }

  // Ordered type helpers.
  @Override public boolean isBounded() { return true; }
  @Override public Object neutralValue() { return 0.0f; }
  @Override public Object minValue() { return Integer.MIN_VALUE; }
  @Override public Object maxValue() { return Integer.MAX_VALUE; }

  @Override
  public Integer getSize() { return Float.SIZE >> 3; }

  @Override
  public Integer getInstanceSize(Object o) { 
    if ( o instanceof Float ) return getSize();
    return -1;
  }

  @Override
  public Object newValue() { return Float.valueOf(0.0f); }

  @Override
  public Object readValue(Tuple t) { return t.readFloat(); }

  @Override
  public void writeValue(Object v, Tuple t) { t.writeFloat((Float) v); } 

  @Override
  public Object parseType(String s) {
      return Float.valueOf(s);
  }
  
}
