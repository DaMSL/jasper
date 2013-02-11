package edu.jhu.cs.damsl.language.core.types;

import edu.jhu.cs.damsl.engine.storage.Tuple;

public class BooleanType extends Type {

  public BooleanType() { super(Boolean.class); }

  @Override
  public Object newValue() { return new Boolean(false); }

  // Unfortunately we use an entire byte to represent booleans in tuples
  // since bytes are the minimal granularity Netty can work with.
  @Override
  public Integer getSize() { return Byte.SIZE >> 3; }
  
  @Override
  public Integer getInstanceSize(Object o) { 
    if ( o instanceof Boolean ) return getSize();
    return -1;
  }

  @Override
  public Object readValue(Tuple t) { 
    return Boolean.valueOf(Byte.valueOf(t.readByte()).intValue() > 0);
  }

  @Override
  public void writeValue(Object v, Tuple t) {
    Boolean b = (Boolean) v;
    t.writeByte(b? 1 : 0);
  }

  @Override
  public Object parseType(String s) {
      return (Object)Boolean.valueOf(s);
  }

}
