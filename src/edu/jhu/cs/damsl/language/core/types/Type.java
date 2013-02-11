package edu.jhu.cs.damsl.language.core.types;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.engine.storage.Tuple;

public abstract class Type implements Serializable {
  protected static final Logger logger = LoggerFactory.getLogger(Type.class);
  Class<?> type;

  // Implementations must define their own naming scheme to map to native types.
  public Type(Class<?> t) {
    type = t; 
  }
  
  @Override
  public boolean equals(Object b) {
    if ( b instanceof Type )
      return getFQTypeName().equals(((Type) b).getFQTypeName());
    return false;
  }
  
  @Override
  public int hashCode() { return getFQTypeName().hashCode(); }
  
  public Class<?> getNativeType() { return type; }
  public String getFQTypeName() { return type.getName(); } 
  public String getTypeName() { return type.getSimpleName(); }

  // Type traits
  public boolean isUnknown() { return type == null; }
  public boolean isBoolean() { return Boolean.class.isAssignableFrom(type); }
  public boolean isNumeric() { return Number.class.isAssignableFrom(type); }
  public boolean isText() { return type == String.class; }
  
  // Value helpers

  // Size in bytes of fixed size types, -1 otherwise.
  public abstract Integer getSize();
  
  // Size in bytes of the object if it matches this type, -1 otherwise.
  public abstract Integer getInstanceSize(Object v);

  // Default value constructor
  public abstract Object newValue();
  
  // Read from tuple
  public abstract Object readValue(Tuple t);
  
  // Write to tuple
  // The subclass implementations may throw a ClassCastException or a
  // Netty IndexOutOfBoundsException if there isn't enough space in the tuple.
  public abstract void writeValue(Object v, Tuple t);

  public abstract Object parseType(String s);
}
