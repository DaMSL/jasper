package edu.jhu.cs.damsl.language.core.types;

import edu.jhu.cs.damsl.engine.storage.Tuple;

public class IntType extends OrderedType {

  public IntType() { super(Integer.class); }

  // Ordered type helpers.
  @Override public boolean isBounded() { return true; }
  @Override public Object neutralValue() { return 0; }
  @Override public Object minValue() { return Integer.MIN_VALUE; }
  @Override public Object maxValue() { return Integer.MAX_VALUE; }

  @Override
  public Integer getSize() { return Integer.SIZE >> 3; }

  @Override
  public Integer getInstanceSize(Object o) { 
    if ( o instanceof Integer ) return getSize();
    return -1;
  }

  @Override
  public Object newValue() { return Integer.valueOf(0); }

  @Override
  public Object readValue(Tuple t) { return t.readInt(); }

  @Override
  public void writeValue(Object v, Tuple t) { t.writeInt((Integer) v); } 

  @Override
  public Object parseType(String s) {
      return (Object)Integer.valueOf(s);
  }

}
