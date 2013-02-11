package edu.jhu.cs.damsl.language.core.types;

import edu.jhu.cs.damsl.engine.storage.Tuple;

public class DoubleType extends OrderedType {

  public DoubleType() { super(Double.class); }

  // Ordered type helpers.
  @Override public boolean isBounded() { return true; }
  @Override public Object neutralValue() { return 0.0d; }
  @Override public Object minValue() { return Double.MIN_VALUE; }
  @Override public Object maxValue() { return Double.MAX_VALUE; }

  @Override
  public Integer getSize() { return Double.SIZE >> 3; }

  @Override
  public Integer getInstanceSize(Object o) { 
    if ( o instanceof Double ) return getSize();
    return -1;
  }

  @Override
  public Object newValue() { return Double.valueOf(0.0); }

  @Override
  public Object readValue(Tuple t) { return t.readDouble(); }

  @Override
  public void writeValue(Object v, Tuple t) { t.writeDouble((Double) v); } 
  
  @Override
  public Object parseType(String s) {
      return (Object)Double.valueOf(s);
  }
  
}
