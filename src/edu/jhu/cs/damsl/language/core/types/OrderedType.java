package edu.jhu.cs.damsl.language.core.types;

public abstract class OrderedType extends Type {

  public OrderedType(Class<?> t) { super(t); }

  // Ordered type helpers.
  public boolean isBounded() { return false; }
  
  public Object neutralValue() throws TypeException {
    throw new TypeException("unordered type: "+getTypeName());
  }

  public Object minValue() throws TypeException { 
    throw new TypeException("unordered type: "+getTypeName());
  }

  public Object maxValue() throws TypeException { 
    throw new TypeException("unordered type: "+getTypeName());
  }
}
